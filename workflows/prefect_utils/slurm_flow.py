import logging
import time
from collections import Counter
from datetime import timedelta
from enum import Enum
from pathlib import Path
from textwrap import dedent as _
from typing import Union

from workflows.prefect_utils.out_of_process_subflow import await_out_of_process_subflow

from prefect import task, flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

from emgapiv2.settings import EMG_CONFIG

try:
    import pyslurm
except:
    logging.warning("No PySlurm available. Patching.")
    import workflows.prefect_utils.pyslurm_patch as pyslurm


CLUSTER_WORKPOOL = "slurm"


class SlurmStatus(str, Enum):
    """
    Possible Slurm Job Statuses. Slurm v 23.
    """

    pending = "PENDING"
    running = "RUNNING"
    completing = "COMPLETING"
    completed = "COMPLETED"
    failed = "FAILED"
    terminated = "TERMINATED"
    suspended = "SUSPENDED"
    stopped = "STOPPED"
    timeout = "TIMEOUT"

    # Custom responses
    unknown = "UNKNOWN"


def slurm_status_is_okay(state: Union[SlurmStatus, str]):
    return state in [
        SlurmStatus.pending,
        SlurmStatus.running,
        SlurmStatus.unknown,
        SlurmStatus.completed,
    ]


def slurm_status_is_finished_successfully(state: Union[SlurmStatus, str]):
    return state in [SlurmStatus.completed]


def slurm_status_is_finished_unsuccessfully(state: Union[SlurmStatus, str]):
    return state in [
        SlurmStatus.failed,
        SlurmStatus.stopped,
        SlurmStatus.timeout,
        SlurmStatus.suspended,
        SlurmStatus.terminated,
    ]


def slurm_status_is_running(state: Union[SlurmStatus, str]):
    return state in [SlurmStatus.running, SlurmStatus.completing]


def slurm_timedelta(delta: timedelta) -> str:
    """
    Rewrite a python timedelta as a slurm duration.
    :param delta: Python timedelta, e.g. `timedelta(minutes=5)
    :return: Slurm duration in days-hours:mins:secs, e.g. `00-00:05:00`
    """
    t_minutes, seconds = divmod(delta.seconds, 60)
    hours, minutes = divmod(t_minutes, 60)
    days = delta.days
    return f"{days:02}-{hours:02}:{minutes:02}:{seconds:02}"


@task(persist_result=True, log_prints=True)
def after_cluster_jobs():
    print(
        "Dummy task to run after cluster jobs, simply to ensure there is a task after pause/resume."
    )


def check_cluster_job(
    job_id: Union[int, str],
) -> str:
    """
    Retrieve the state (e.g. RUNNING) of a cluster job on slurm.
    :param job_id: Slurm job ID e.g. 10101 or 10101_1
    :return: state of the job, as one of the string values of SlurmStatus.
    """
    logger = get_run_logger()
    try:
        job = pyslurm.db.Job(job_id).load(job_id)
    except pyslurm.core.error.RPCError:
        logger.warning(f"Error talking to slurm for job {job_id}")
        return SlurmStatus.unknown.value
    logger.info(f"SLURM status of {job_id = } is {job.state}")
    job_log_path = Path(EMG_CONFIG.slurm.default_workdir) / Path(f"slurm-{job_id}.out")
    if job_log_path.exists():
        with open(job_log_path, "r") as job_log:
            full_log = job_log.readlines()
            log = "\n".join(full_log[-EMG_CONFIG.slurm.job_log_tail_lines :])
            logger.info(
                _(
                    f"""\
                    Slurm Job Stdout Log (last {EMG_CONFIG.slurm.job_log_tail_lines} lines of {len(full_log)}):
                    ----------
                    <<LOG>>
                    ----------
                    """
                ).replace("<<LOG>>", log)
            )
    else:
        logger.info(f"No Slurm Job Stdout available at {job_log_path}")

    return job.state


def get_cluster_state_counts() -> dict[SlurmStatus, int]:
    logger = get_run_logger()
    try:
        our_jobs = pyslurm.db.JobFilter(users=[EMG_CONFIG.slurm.user])
        jobs = pyslurm.db.Jobs.load(our_jobs)
    except pyslurm.core.error.RPCError:
        logger.warning(f"Error talking to slurm")
        return {}
    logger.info(f"SLURM job total count: {len(jobs)}")
    return Counter([job.state for job in jobs.values()])


def cluster_can_accept_jobs() -> int:
    """
    Does the cluster have "space" for more pending jobs? And how many?
    :return: Zero if there is no space. Otherwise, positive int of how many jobs can be taken.
    """
    current_job_state_counts = get_cluster_state_counts()
    job_load = (
        current_job_state_counts[SlurmStatus.running]
        + current_job_state_counts[SlurmStatus.pending]
    )
    space = EMG_CONFIG.slurm.incomplete_job_limit - job_load
    return max(space, 0)


@task(
    task_run_name="Job submission: {name}",
    log_prints=True,
    persist_result=True,
)
def start_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    **kwargs,
) -> str:
    """
    Run a command on the HPC Cluster by submitting it as a Slurm job.
    :param name: Name for the job (both on Slurm and Prefect), e.g. "Run analysis pipeline for x"
    :param command: Shell-level command to run, e.g. "nextflow run my-pipeline.nf --sample x"
    :param expected_time: A timedelta after which the job will be killed if not done.
    This affects Prefect's polling interval too. E.g.  `timedelta(minutes=5)`
    :param memory: Maximum memory the job may use. In MB, or with a prefix. E.g. `100` or `10G`.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription
    :return: Job ID of the slurm job.
    """
    logger = get_run_logger()
    script = _(
        f"""\
        #!/bin/bash
        {command}
        """
    )
    logger.info(f"Will run the script ```{script}```")
    desc = pyslurm.JobSubmitDescription(
        name=name,
        time_limit=slurm_timedelta(expected_time),
        memory_per_node=memory,
        script=script,
        working_directory=EMG_CONFIG.slurm.default_workdir,
        **kwargs,
    )
    job_id = desc.submit()
    logger.info(f"Submitted as slurm job {job_id}")

    create_markdown_artifact(
        key="slurm-job-submission",
        markdown=_(
            f"""\
            # Slurm job {job_id}
            Submitted a script to Slurm cluster:
            ~~~
            <<SCRIPT>>
            ~~~
            It will be terminated by Slurm if not done in {slurm_timedelta(expected_time)}.
            """
        ).replace("<<SCRIPT>>", script),
    )

    return job_id


class ClusterJobFailedException(Exception):
    ...


class ClusterPendingJobsLimitReachedException(Exception):
    ...


@task(
    persist_result=True,
    retries=EMG_CONFIG.slurm.default_submission_attempts_limit,
    retry_delay_seconds=EMG_CONFIG.slurm.default_seconds_between_submission_attempts,
)
def _delay_until_cluster_has_space() -> int:
    """
    Run once (by persisting the result of this flow) at the start of a cluster job,
    to potentially wait until the slurm cluster queue is sufficiently small for us to submit
    a new job.
    TODO: add "pressure" based on creation time of this flow, to enable prioritisation
    TODO:   or use concurrency limit on this
    :return: Free space (as number of jobs) below our limit. Will fail if above limit.
    """
    if not (space_on_cluster := cluster_can_accept_jobs()):
        raise ClusterPendingJobsLimitReachedException
    return space_on_cluster


@flow(flow_run_name="Cluster job: {name}", persist_result=True, retries=10)
async def run_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    **kwargs,
):
    """
    Run and wait for a job on the HPC cluster.
    This flow will usually be paused and resumed many times,
    because a separate monitor job will occasionally wake up this flow to monitor its HPC job.

    :param name: Name for the job on slurm.
    :param command: Shell-level command to run.
    :param expected_time: A timedelta after which the job will be killed if unfinished.
    :param memory: Max memory the job may use. In MB, or with a suffix. E.g. `100` or `10G`.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription.
    :return: Django model instance representing the link between Prefect and Slurm.
    """
    logger = get_run_logger()

    # Potentially wait some time if our cluster queue is very full
    space_on_cluster = _delay_until_cluster_has_space()

    # Submit the job to cluster.
    # This is a persisted result, so if this flow is retried, a new job will *not* be submitted.
    # Rather, the original job_id will be returned.
    job_id = start_cluster_job(
        name=name,
        command=command,
        expected_time=expected_time,
        memory=memory,
        wait_for=space_on_cluster,
        **kwargs,
    )

    # Wait for job completion
    is_job_in_terminal_state = False
    while not is_job_in_terminal_state:
        job_state = check_cluster_job(job_id)
        if slurm_status_is_finished_successfully(job_state):
            logger.info(f"Job {job_id} finished successfully.")
            is_job_in_terminal_state = True

        if slurm_status_is_finished_unsuccessfully(job_state):
            raise ClusterJobFailedException()

        else:
            logger.debug(
                f"Job {job_id} is still running. "
                f"Sleeping for {EMG_CONFIG.slurm.default_seconds_between_submission_attempts} seconds."
            )
            time.sleep(EMG_CONFIG.slurm.default_seconds_between_job_checks)

    return job_id


# async def run_cluster_jobs(
#     name_pattern: str,
#     command_pattern: str,
#     jobs_args: List[Dict],
#     expected_time: timedelta,
#     memory: Union[int, str],
#     **kwargs,
# ):
#     """
#     Run multiple similar jobs on the HPC cluster, by submitting a common pattern (with different values interpolated) and waiting for them all to complete.
#     :param name_pattern: Name for each job. Values from each element of `args` will be interpolated.
#     :param command_pattern: Shell-level command to run for each element of `args`. Treated as f-string with `args` interpolated.
#     :param jobs_args: List of dicts. Jobs will be created for each element of list. Dict keys are interpolations for `name_pattern` and `command_pattern`.
#     :param expected_time:  A timedelta after which the job will be killed if not done.
#     This affects Prefect's polling interval too. E.g.  `timedelta(minutes=5)`
#     :param memory: Maximum memory the job may use. In MB, or with a prefix. E.g. `100` or `10G`.
#     :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription
#     :return: List of outputs from each job.
#     """
#     logger = get_run_logger()
#
#     jobs = [
#         await run_cluster_job(
#             name=name_pattern.format(**job_args),
#             command=command_pattern.format(**job_args),
#             expected_time=expected_time,
#             memory=memory,
#             **kwargs,
#         )
#         for job_args in jobs_args
#     ]
#     logger.info(f"{len(jobs)} jobs (potentially) submitted to cluster")
#
#     await create_table_artifact(
#         key="slurm-group-of-jobs-submission",
#         table=[
#             {
#                 "Name": name_pattern.format(**job_args),
#                 "Command": command_pattern.format(**job_args),
#                 "Slurm Job ID": job.cluster_job_id,
#                 "Prefect Subflow ID": job.prefect_flow_run_id,
#                 "Initial state": job.last_known_state,
#             }
#             for job_args, job in zip(jobs_args, jobs)
#         ],
#         description="Jobs submitted to Slurm",
#     )
#
#     return jobs


@task(persist_result=True)
async def await_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    **kwargs,
):
    """
    Convenience function for a Prefect flow to wait for an out-of-process command on the HPC cluster.

    This orchestrates a few flows necessary to monitor the HPC job, its wrapper Prefect flow, and coordinate a
    return to the execution of the calling flow when the job is done.

    An instance of PrefectInitiatedHPCJob Django model will also be created.

    The calling Prefect flow will be suspended when this job is launched.

    :param name: Name for the job on slurm.
    :param command: Shell-level command to run.
    :param expected_time: A timedelta after which the job will be killed if unfinished.
    :param memory: Max memory the job may use. In MB, or with a suffix. E.g. `100` or `10G`.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription.
    :return: Subflow-run ID of the Prefect flow which is managing the slurm job.
    """
    return await await_out_of_process_subflow(
        subflow_deployment_name="run-cluster-job/run_cluster_job_deployment",
        parameters={
            "name": name,
            "command": command,
            "expected_time": expected_time,
            "memory": memory,
            **kwargs,
        },
    )
