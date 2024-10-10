import hashlib
import logging
import os
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from textwrap import dedent as _
from typing import Callable, List, Optional, Union

from django.utils.text import slugify
from prefect import Flow, State, flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.client.schemas import FlowRun
from prefect.context import TaskRunContext
from prefect.runtime import flow_run
from prefect.variables import Variable
from pydantic import AnyUrl
from pydantic_core import Url

from emgapiv2.settings import EMG_CONFIG
from workflows.data_io_utils.filenames import file_path_shortener
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash

if "PYTEST_CURRENT_TEST" in os.environ:
    logging.warning("Unit testing, so patching pyslurm.")
    import workflows.prefect_utils.pyslurm_patch as pyslurm
else:
    try:
        import pyslurm
    except:
        logging.warning("No PySlurm available. Patching.")
        import workflows.prefect_utils.pyslurm_patch as pyslurm


CLUSTER_WORKPOOL = "slurm"
SLURM_JOB_ID = "Slurm Job ID"
FINAL_SLURM_STATE = "Final Slurm state"


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
    cancelled = "CANCELLED"

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
        SlurmStatus.cancelled,
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
    job_log_path = Path(job.working_directory) / Path(f"slurm-{job_id}.out")
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


def maybe_get_nextflow_tower_browse_url(command: str) -> Optional[AnyUrl]:
    """
    If the command looks like a nextflow run with tower enabled and an explicitly defined name,
    return the Nextflow Tower URL for it (to be browsed).
    :param command: A command-line instruction e.g. nextflow run....
    :return: A Nextflow Tower / Seqera Platform URL, or None
    """
    if "nextflow run" in command and "-tower" in command and "-name" in command:
        try:
            wf_name = command.split("-name")[1].strip().split(" ")[0]
        except KeyError:
            logging.warning(
                f"Could not determine nextflow workflow run name from {command}"
            )
            return
        return Url(
            f"https://cloud.seqera.io/orgs/{EMG_CONFIG.slurm.nextflow_tower_org}/workspaces/{EMG_CONFIG.slurm.nextflow_tower_workspace}/watch?search={wf_name}"
        )


def _ensure_absolute_workdir(workdir):
    path = Path(workdir)
    if not path.is_absolute():
        base_path = Path(EMG_CONFIG.slurm.default_workdir)
        return base_path / path
    return path


@task(
    task_run_name="Job submission: {name}",
    log_prints=True,
    persist_result=True,
    cache_key_fn=context_agnostic_task_input_hash,
)
def start_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    workdir: Optional[Union[Path, str]] = None,
    make_workdir_first: bool = True,
    hash: str = "",
    **kwargs,
) -> str:
    """
    Run a command on the HPC Cluster by submitting it as a Slurm job.
    :param name: Name for the job (both on Slurm and Prefect), e.g. "Run analysis pipeline for x"
    :param command: Shell-level command to run, e.g. "nextflow run my-pipeline.nf --sample x"
    :param expected_time: A timedelta after which the job will be killed if not done.
        This affects Prefect's polling interval too. E.g.  `timedelta(minutes=5)`
    :param memory: Maximum memory the job may use. In MB, or with a prefix. E.g. `100` or `10G`.
    :param workdir: Work dir for the job (pathlib.Path, or str). Otherwise, a default will be used based on the name.
    :param make_workdir_first: Make the work dir first, on the SUBMITTER machine.
        Usually this is desirable, except in cases where you're launching a job to a slurm node which has diff fs mounts.
    :param hash: A string hash, used along with other params to determine if this job is "new" or already been/being run.
        Basically, this is a cache-buster.
        Common use case: a hash of some input files referenced by 'command',
        that might have changed even though the command itself has not.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription
    :return: Job ID of the slurm job.
    """
    logger = get_run_logger()
    logger.debug(f"Hash is {hash}")

    job_workdir = workdir

    if not job_workdir:
        # Make a unique workdir in the default location
        unique_job_folder = slugify(
            f"{name}-{datetime.now().isoformat()}-{str(uuid.uuid4()).split('-')[-1]}"
        ).upper()
        job_workdir = Path(EMG_CONFIG.slurm.default_workdir) / Path(unique_job_folder)

    # If workdir was given as relative, make it absolute using default workdir as basepath
    job_workdir = _ensure_absolute_workdir(job_workdir)
    if make_workdir_first and not job_workdir.exists():
        os.mkdir(job_workdir)

    script = _(
        f"""\
        #!/bin/bash
        set -euo pipefail
        {command}
        """
    )
    logger.info(f"Will run the script ```{script}```")
    desc = pyslurm.JobSubmitDescription(
        name=name,
        time_limit=slurm_timedelta(expected_time),
        memory_per_node=memory,
        script=script,
        working_directory=str(job_workdir),
        **kwargs,
    )
    job_id = desc.submit()
    logger.info(f"Submitted as slurm job {job_id}")

    nf_link = maybe_get_nextflow_tower_browse_url(command)
    nf_link_markdown = f"[Watch Nextflow Workflow]({nf_link})" if nf_link else ""

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
            Slurm working dir is {job_workdir}.
            {nf_link_markdown}
            """
        ).replace("<<SCRIPT>>", script),
    )

    return job_id


def cancel_cluster_job(name: str):
    """
    Finds a job running slurm (by name) and cancels it provided there is exactly one match.
    :param name: The job name as submitted.
    :return:
    """
    jobs = pyslurm.db.Jobs.load(
        db_filter=pyslurm.db.JobFilter(names=[name], users=[EMG_CONFIG.slurm.user])
    )
    jobs_to_cancel = [
        job.job_id for job in jobs.values() if job.state == SlurmStatus.running
    ]

    if len(jobs_to_cancel) == 1:
        try:
            job_id = int(jobs_to_cancel[0])
        except ValueError:
            raise ValueError(
                "Cannot cancel job array jobs - job id must be integer like"
            )

        print(f"Found one running job to cancel: {job_id}")
        pyslurm.Job(job_id).load(job_id).cancel()

    else:
        raise Exception(
            f"Found {len(jobs_to_cancel)} matching jobs to cancel for name {name}. Not cancelling."
        )


class ClusterJobFailedException(Exception): ...


class ClusterPendingJobsLimitReachedException(Exception): ...


def _cluster_delay_key(context: TaskRunContext, parameters: dict) -> str:
    """
    Creates a cache key that prevents the cluster delay task from running again for the same flow.
    I.e., will not be impactful on a re-run of the parent flow.
    """
    return f"cluster-delay-marker-{context.task_run.flow_run_id}"


@task(
    retries=EMG_CONFIG.slurm.default_submission_attempts_limit,
    retry_delay_seconds=EMG_CONFIG.slurm.default_seconds_between_submission_attempts,
    cache_key_fn=_cluster_delay_key,
)
def _delay_until_cluster_has_space() -> int:
    """
    Run once (by caching the result of this task) at the start of a cluster job,
    to potentially wait until the slurm cluster queue is sufficiently small for us to submit
    a new job.
    TODO: add "pressure" based on creation time of this flow, to enable prioritisation
    TODO:   or use concurrency limit on this
    :param delay_key: a string key to prevent another delay occurring once one already has.
    :return: Free space (as number of jobs) below our limit. Will fail if above limit.
    """
    if not (space_on_cluster := cluster_can_accept_jobs()):
        raise ClusterPendingJobsLimitReachedException
    return space_on_cluster


def cancel_cluster_jobs_if_flow_cancelled(
    the_flow: Flow, the_flow_run: FlowRun, state: State
):
    if "name" in the_flow_run.parameters:
        job_names = [the_flow_run.parameters.get("name")]
    elif "names" in the_flow_run.parameters:
        job_names = [the_flow_run.parameters.get("names")]
    else:
        raise UserWarning(
            f"Flow run {the_flow_run.id} had no params called 'name' or 'names' so don't know what jobs to cancel"
        )

    print(f"Will try to cancel jobs matching the job names {job_names}")
    for job_name in job_names:
        cancel_cluster_job(job_name)


@task
def compute_hash_of_input_file(
    input_files_to_hash: Optional[List[Union[Path, str]]] = None
) -> str:
    logger = get_run_logger()
    input_files_hash = hashlib.new("blake2b")

    for input_file in input_files_to_hash or []:
        if not Path(input_file).is_file():
            logger.warning(f"Did not find a file to hash at {input_file}. Ignoring it.")
            continue
        with open(input_file, "rb") as f:
            for chunk in iter(
                lambda: f.read(131072), b""
            ):  # 131072 is rsize on EBI /nfs/production, so slightly optimised for that
                input_files_hash.update(chunk)
    return input_files_hash.hexdigest()


@flow(
    flow_run_name="Cluster job: {name}",
    persist_result=True,
    retries=10,
    on_cancellation=[cancel_cluster_jobs_if_flow_cancelled],
)
async def run_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    environment: Union[dict, str],
    resubmit_even_if_identical: bool = False,
    input_files_to_hash: Optional[List[Union[Path, str]]] = None,
    **kwargs,
) -> str:
    """
    Run and wait for a job on the HPC cluster.

    :param name: Name for the job on slurm, e.g. "job 1"
    :param command: Shell-level command to run, e.g. "touch 1.txt"
    :param expected_time: A timedelta after which the job will be killed if unfinished, e.g. timedelta(days=1)
    :param memory: Max memory the job may use. In MB, or with a suffix. E.g. `100` or `10G`.
    :param environment: Dictionary of environment variables to pass to job, or string in format of sbatch --export
        (see https://slurm.schedmd.com/sbatch.html). E.g. `TOWER_ACCESSION_TOKEN`
    :param resubmit_even_if_identical: Boolean which if True, will force a new cluster job to be created even if
        an identical one was already run and still exists in the cache.
    :param input_files_to_hash: Optional list of filepaths,
        whose contents will be hashed to determine if this job is identical to another.
        Note that the hash is done on the node where this flow runs, not the node where the job (may) run.
        This means hashes can't be computed for files only accessible to certain partitions (like datamover nodes).
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription.
    :return: Slurm job ID once finished.
    """
    logger = get_run_logger()

    # Potentially wait some time if our cluster queue is very full
    space_on_cluster = _delay_until_cluster_has_space()

    # Submit the job to cluster.
    # This is a cached result, so if this flow is retried, a new job will *not* be submitted.
    # Rather, the original job_id will be returned.

    input_files_hash = compute_hash_of_input_file(input_files_to_hash)

    cluster_job_start_args = {
        "name": name,
        "command": command,
        "expected_time": expected_time,
        "memory": memory,
        "environment": environment,
        "hash": input_files_hash,
        **kwargs,
    }

    if resubmit_even_if_identical:
        logger.warning(f"Ignoring any potentially cached previous runs of this job")
        job_id = start_cluster_job.with_options(refresh_cache=True)(
            **cluster_job_start_args, wait_for=space_on_cluster
        )
    else:
        job_id = start_cluster_job(
            **cluster_job_start_args,
            wait_for=space_on_cluster,
        )
    logger.info(f"{job_id=}")

    restart_instruction: Variable = await Variable.get(f"restart_{job_id}")
    logger.info(
        f"Checked restart variable at restart_{job_id}: found {restart_instruction}"
    )
    if restart_instruction and str(restart_instruction.value).lower() == "true":
        # A user has explicitly marked this slurm job for restarting, so refresh the cache (resubmit it)
        logger.warning(
            f"Resubmitting slurm job `{name}`, because jobid {job_id} was marked for restart."
        )
        resubmit_job_id = start_cluster_job.with_options(refresh_cache=True)(
            **cluster_job_start_args, wait_for=space_on_cluster
        )
        logger.info(f"Resubmitted slurm job id {job_id} as {resubmit_job_id}")
        await Variable.set(
            f"restart_{job_id}", f"Resubmitted as {resubmit_job_id}", overwrite=True
        )
        job_id = resubmit_job_id

    # Wait for job completion
    # Resumability: if this flow was re-run / restarted for some reason, or the exact same cluster job was sent later,
    #  we should have gotten  back an existing slurm job_id of a previous run of it. And therefore the first status
    #  check will just tell us the job finished immediately / it'll wait for the EXISTING job to finish.
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


def _default_dirname(name, command):
    return slugify(name).upper()


@flow(
    flow_run_name="Cluster jobs",
    persist_result=True,
    retries=10,
    on_cancellation=[cancel_cluster_jobs_if_flow_cancelled],
)
async def run_cluster_jobs(
    names: List[str],
    commands: List[str],
    expected_time: timedelta,
    memory: Union[int, str],
    environment: Union[dict, str],
    workdirs: Union[List[str], Callable[[str, str], str]] = _default_dirname,
    raise_on_job_failure: bool = True,
    **kwargs,
) -> list[dict[str, str]]:
    """
    Run and wait for a set of jobs on the HPC cluster.
    :param names: Names for each job, e.g. ["job 1", "job 2"...]
    :param commands: Shell-level command to run for each job, e.g. ["touch 1.txt", "touch 2.txt", ...]
    :param expected_time: A timedelta after which the jobs will be killed if unfinished.
    :param memory: Max memory the jobs may use. In MB, or with a suffix. E.g. `100` or `10G`.
    :param environment: Dictionary of environment variables to pass to job, or string in format of sbatch --export
        (see https://slurm.schedmd.com/sbatch.html). E.g. `TOWER_ACCESSION_TOKEN`
    :param workdirs: Unique work directory for each job. Can either be a list like ["job-1", "job-2", ...],
        or a function that turn each *name* and *command* pair into a key, e.g. lambda nm, cmd: nm" (which is the default).
    :param raise_on_job_failure: Whether to fail this flow if ANY slurm job fails.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription.
    :return: List of jobs (same order as jobs_args), with a dict of final info. Included "Final Slurm state" key.
    """
    logger = get_run_logger()

    assert len(names) == len(commands)

    # Potentially wait some time if our cluster queue is very full
    space_on_cluster = _delay_until_cluster_has_space()
    # TODO: probably should wait for additional space for multi jobs...

    # Submit the jobs to cluster.
    # These are persisted result, so if this flow is retried, new jobs will *not* be submitted.
    # Rather, the original job_ids will be returned.
    _workdirs = (
        workdirs
        if type(workdirs) is list
        else [workdirs(n, c) for n, c in zip(names, commands)]
    )
    job_ids = [
        start_cluster_job(
            name=name,
            command=command,
            expected_time=expected_time,
            memory=memory,
            workdir=workdir,
            environment=environment,
            wait_for=space_on_cluster,
            **kwargs,
        )
        for name, command, workdir in zip(names, commands, _workdirs)
    ]

    logger.info(f"{len(job_ids)} jobs submitted to cluster")

    await create_table_artifact(
        key="slurm-group-of-jobs-submission",
        table=[
            {
                "Name": name,
                "Command": command,
                "Slurm Job ID": job_id,
                "Working directory": workdir,
                "Observe URL": str(maybe_get_nextflow_tower_browse_url(command)),
            }
            for name, command, job_id, workdir in zip(
                names, commands, job_ids, _workdirs
            )
        ],
        description="Jobs submitted to Slurm",
    )

    jobs_are_terminal = {job_id: False for job_id in job_ids}

    # Wait for all jobs to complete.
    # If we are here in a retry or even a later duplicate run (trying to run IDENTICAL jobs),
    #  we will have been given back cached job_ids above.
    #  Therefore, these status check loops will return the state of PREVIOUSLY launched jobs.
    #  This is usually desirable for resumability / efficiency.
    while not all(jobs_are_terminal.values()):
        job_states = {job_id: check_cluster_job(job_id) for job_id in job_ids}

        for job_id, job_state in job_states.items():
            # job is newly finished
            if (
                slurm_status_is_finished_successfully(job_state)
                and not jobs_are_terminal[job_id]
            ):
                logger.info(f"Job {job_id} finished successfully.")
                jobs_are_terminal[job_id] = True

            elif (
                slurm_status_is_finished_unsuccessfully(job_state)
                and not jobs_are_terminal[job_id]
            ):
                logger.info(f"Job {job_id} finished unsuccessfully.")
                jobs_are_terminal[job_id] = True
                if raise_on_job_failure:
                    raise ClusterJobFailedException()
                else:
                    logger.warning(
                        f"Job {job_id} finished unsuccessfully, but this is being allowed."
                    )

            else:
                logger.debug(f"Job {job_id} is still running.")

        if not all(jobs_are_terminal.values()):
            logger.debug(
                f"Some jobs are still running. "
                f"Sleeping for {EMG_CONFIG.slurm.default_seconds_between_job_checks} seconds."
            )
            time.sleep(EMG_CONFIG.slurm.default_seconds_between_job_checks)

    results_table = [
        {
            "Name": name,
            "Command": command,
            SLURM_JOB_ID: job_id,
            FINAL_SLURM_STATE: check_cluster_job(job_id),
        }
        for name, command, job_id in zip(names, commands, job_ids)
    ]

    await create_table_artifact(
        key="slurm-group-of-jobs-results",
        table=results_table,
        description="Jobs results from Slurm",
    )

    return results_table


def move_data_flow_name() -> str:
    source = flow_run.parameters["source"]
    target = flow_run.parameters["target"]
    return f"Move data: {file_path_shortener(source, 1, 10)} > {file_path_shortener(target, 1, 10)}"


@flow(flow_run_name=move_data_flow_name)
async def move_data(source: str, target: str, move_command: str = "cp", **kwargs):
    """
    Move files on the cluster filesystem.
    This uses a slurm job running on the datamover partition.

    :param source: fully qualified path of the source location (file or folder)
    :param target: fully qualified path of the target location (file or folder)
    :param move_command: tool command for the move. Default is `cp`, but could be `mv` or `rsync` etc.
    :param kwargs: Other keywords to pass to run_cluster_job
        (e.g. expected_time, memory, or other slurm job-description parameters)
    :return: Job ID of the datamover job.
    """
    expected_time = kwargs.pop("expected_time", timedelta(hours=2))
    memory = kwargs.pop("memory", "1G")

    if not "environment" in kwargs:
        kwargs["environment"] = {}

    return await run_cluster_job(
        name=f"Move {file_path_shortener(source)} to {file_path_shortener(target)}",
        command=f"{move_command} {source} {target}",
        expected_time=expected_time,
        memory=memory,
        resubmit_even_if_identical=True,
        partitions=[EMG_CONFIG.slurm.datamover_paritition],
        **kwargs,
    )
