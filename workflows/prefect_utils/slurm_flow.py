import logging
import time
from collections import Counter
from datetime import timedelta
from enum import Enum
from pathlib import Path
from textwrap import dedent as _, indent
from typing import Union, List, Dict

import django
from asgiref.sync import sync_to_async
from django.db.models import QuerySet

django.setup()

from prefect import task, flow, get_run_logger, suspend_flow_run, resume_flow_run
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.runtime import flow_run

from emgapiv2.settings import EMG_CONFIG
from workflows.models import PrefectInitiatedHPCJob

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


def slurm_status_is_okay(state: SlurmStatus):
    return state in [
        SlurmStatus.pending,
        SlurmStatus.running,
        SlurmStatus.unknown,
        SlurmStatus.completed,
    ]


def slurm_status_is_finished_successfully(state: SlurmStatus):
    return state in [SlurmStatus.completed]


def slurm_status_is_finished_unsuccessfully(state: SlurmStatus):
    return state in [
        SlurmStatus.failed,
        SlurmStatus.stopped,
        SlurmStatus.timeout,
        SlurmStatus.suspended,
        SlurmStatus.terminated,
    ]


def slurm_status_is_running(state: SlurmStatus):
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
    job_id: int,
):
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
            log = "\n".join(job_log.readlines())
            logger.info(
                _(
                    f"""\
                    Slurm Job Stdout Log:
                    ----------
                    {indent(log, ' ' * 20)}
                    ----------
                    """
                )
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
    :return: Zero if there is no space. Otherwise positive int of how many jobs can be taken.
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
):
    """
    Run a command on the HPC Cluster by submitting it as a Slurm job.
    :param name: Name for the job (both on Slurm and Prefect), e.g. "Run analysis pipeline for x"
    :param command: Shell-level command to run, e.g. "nextflow run my-pipeline.nf --sample x"
    :param expected_time: A timedelta after which the job will be killed if not done.
    This affects Prefect's polling interval too. E.g.  `timedelta(minutes=5)`
    :param memory: Maximum memory the job may use. In MB, or with a prefix. E.g. `100` or `10G`.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription
    :return: A promise (Prefect Future) for the submitted job's future state.
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
            {indent(script, ' ' * 12)}
            ~~~
            It will be terminated by Slurm if not done in {slurm_timedelta(expected_time)}.
            """
        ),
    )
    return job_id


class ClusterJobFailedException(Exception):
    ...


@task(
    persist_result=False,
    task_run_name="Monitor slurm: {job.prefect_flow_run_id}",
    log_prints=True,
)
async def _monitor_cluster_job(job: PrefectInitiatedHPCJob) -> PrefectInitiatedHPCJob:
    if job.last_known_state == job.JobStates.COMPLETED_SUCCESS:
        # Already finished.
        pass

    if (
        job.cluster_job_id is not None
        and job.last_known_state is not job.JobStates.COMPLETED_SUCCESS
    ):
        # On slurm already so check if now finished
        slurm_status = check_cluster_job(job.cluster_job_id)
        if slurm_status_is_finished_successfully(slurm_status):
            job.last_known_state = job.JobStates.COMPLETED_SUCCESS
        elif slurm_status_is_finished_unsuccessfully(slurm_status):
            job.last_known_state = job.JobStates.COMPLETED_FAIL
        elif slurm_status_is_running(slurm_status):
            job.last_known_state = job.JobStates.CLUSTER_RUNNING
        elif slurm_status == SlurmStatus.pending:
            job.last_known_state = job.JobStates.CLUSTER_PENDING
        job.save()

    if job.last_known_state in [
        job.JobStates.CLUSTER_PENDING,
        job.JobStates.CLUSTER_RUNNING,
        job.JobStates.PRE_CLUSTER_PENDING,
    ]:
        # parent flow should pause again to wait for job
        # this task isn't persisted so will run again with next flow run
        await suspend_flow_run(timeout=None)

    if job.last_known_state == job.JobStates.COMPLETED_FAIL:
        # Bubble up failed jobs to flow level failures, to be handled by any parent flow logic
        raise ClusterJobFailedException(
            f"Job {job.cluster_job_id} failed on the cluster"
        )

    return job


@flow(flow_run_name="Cluster job: {name}")
async def run_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    **kwargs,
) -> PrefectInitiatedHPCJob:
    """
    Run and wait for a job on the HPC cluster.

    :param name: Name for the job on slurm.
    :param command: Shell-level command to run.
    :param expected_time: A timedelta after which the job will be killed if unfinished.
    :param memory: Max memory the job may use. In MB, or with a suffix. E.g. `100` or `10G`.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription.
    :return: Django model instance representing the link between Prefect and Slurm.
    """
    logger = get_run_logger()
    job, is_new = await sync_to_async(PrefectInitiatedHPCJob.objects.get_or_create)(
        prefect_flow_run_id=flow_run.id,
        defaults={
            "command": command,
        },
    )

    if is_new or job.last_known_state == job.JobStates.PRE_CLUSTER_PENDING:
        # Need to submit job to slurm if there is space
        if cluster_can_accept_jobs():
            logger.info("There is space on the cluster for this job.")
            job_id = start_cluster_job(
                name=name,
                command=command,
                expected_time=expected_time,
                memory=memory,
                **kwargs,
            )
            job.cluster_job_id = job_id
            job.last_known_state = job.JobStates.CLUSTER_PENDING
            job.save()
        else:
            logger.info("No space on cluster right now.")
        await suspend_flow_run(timeout=None)
    else:
        await _monitor_cluster_job(job)

    after_cluster_jobs()

    return job


async def run_cluster_jobs(
    name_pattern: str,
    command_pattern: str,
    jobs_args: List[Dict],
    expected_time: timedelta,
    memory: Union[int, str],
    status_checks_limit: int = EMG_CONFIG.slurm.default_job_status_checks_limit,
    **kwargs,
):
    """
    Run multiple similar jobs on the HPC cluster, by submitting a common pattern (with different values interpolated) and waiting for them all to complete.
    :param name_pattern: Name for each job. Values from each element of `args` will be interpolated.
    :param command_pattern: Shell-level command to run for each element of `args`. Treated as f-string with `args` interpolated.
    :param jobs_args: List of dicts. Jobs will be created for each element of list. Dict keys are interpolations for `name_pattern` and `command_pattern`.
    :param expected_time:  A timedelta after which the job will be killed if not done.
    This affects Prefect's polling interval too. E.g.  `timedelta(minutes=5)`
    :param memory: Maximum memory the job may use. In MB, or with a prefix. E.g. `100` or `10G`.
    :param status_checks_limit:  How many times to check on the job between submission and the expected finish time.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription
    :return: List of outputs from each job.
    """
    logger = get_run_logger()

    jobs = [
        await run_cluster_job(
            name=name_pattern.format(**job_args),
            command=command_pattern.format(**job_args),
            expected_time=expected_time,
            memory=memory,
            **kwargs,
        )
        for job_args in jobs_args
    ]
    logger.info(f"{len(jobs)} jobs (potentially) submitted to cluster")

    await create_table_artifact(
        key="slurm-group-of-jobs-submission",
        table=[
            {
                "Name": name_pattern.format(**job_args),
                "Command": command_pattern.format(**job_args),
                "Slurm Job ID": job.cluster_job_id,
                "Prefect Subflow ID": job.prefect_flow_run_id,
                "Initial state": job.last_known_state,
            }
            for job_args, job in zip(jobs_args, jobs)
        ],
        description="Jobs submitted to Slurm",
    )

    return jobs


def get_next_jobs_to_submit(n: int) -> Union[QuerySet, List[PrefectInitiatedHPCJob]]:
    return PrefectInitiatedHPCJob.objects.filter(
        last_known_state=PrefectInitiatedHPCJob.JobStates.PRE_CLUSTER_PENDING
    ).order_by("created")[:n]


def get_jobs_on_cluster() -> Union[QuerySet, List[PrefectInitiatedHPCJob]]:
    return PrefectInitiatedHPCJob.objects.filter(
        last_known_state__in=[
            PrefectInitiatedHPCJob.JobStates.CLUSTER_PENDING,
            PrefectInitiatedHPCJob.JobStates.CLUSTER_RUNNING,
        ]
    )


def _make_timely_monitor_name():
    return f"Job state at {time.strftime('%Y-%m-%d-%H:%M:%S')}UTC"


@flow(flow_run_name=_make_timely_monitor_name)
async def monitor_cluster():
    potentially_complete_jobs = get_jobs_on_cluster()
    async for job in potentially_complete_jobs:
        # get prefect flow and resume it
        # resuming it should cause a check of slurm
        await resume_flow_run(job.prefect_flow_run_id)
        # additional pause so that loads of flows don't resume at some time,
        #  all of which will query slurm around the same time...
        time.sleep(EMG_CONFIG.slurm.wait_seconds_between_slurm_flow_resumptions)

    job_space = cluster_can_accept_jobs()
    # TODO: this isn't perfect because e.g. head nextflow jobs may launch many follow-on jobs
    # For now will need to set our job limit very conservatively

    next_jobs_to_submit = get_next_jobs_to_submit(job_space)
    async for job in next_jobs_to_submit:
        # as above, resume the flow and it should submit the job to slurm now (unless space vanishes in the meantime)
        await resume_flow_run(job.prefect_flow_run_id)
        time.sleep(EMG_CONFIG.slurm.wait_seconds_between_slurm_flow_resumptions)
