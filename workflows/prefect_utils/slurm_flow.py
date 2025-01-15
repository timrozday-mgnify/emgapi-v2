import hashlib
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent as _
from typing import List, Optional, Type, Union

from django.conf import settings
from django.urls import reverse
from django.utils.text import slugify
from prefect import Flow, State, flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.client.schemas import FlowRun
from prefect.runtime import flow_run
from prefect.variables import Variable
from pydantic import AnyUrl
from pydantic_core import Url

from emgapiv2.log_utils import mask_sensitive_data as safe
from workflows.models import OrchestratedClusterJob
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.slurm_limits import delay_until_cluster_has_space
from workflows.prefect_utils.slurm_policies import _SlurmResubmitPolicy
from workflows.prefect_utils.slurm_status import (
    SlurmStatus,
    slurm_status_is_finished_successfully,
    slurm_status_is_finished_unsuccessfully,
)

if "PYTEST_CURRENT_TEST" in os.environ:
    logging.warning("Unit testing, so patching pyslurm.")
    import workflows.prefect_utils.pyslurm_patch as pyslurm
else:
    try:
        import pyslurm
    except:
        logging.warning("No PySlurm available. Patching.")
        import workflows.prefect_utils.pyslurm_patch as pyslurm

EMG_CONFIG = settings.EMG_CONFIG

CLUSTER_WORKPOOL = "slurm"
SLURM_JOB_ID = "Slurm Job ID"


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


def check_cluster_job(
    job_id: Union[int, str],
) -> str:
    """
    Retrieve the state (e.g. RUNNING) of a cluster job on slurm.
    Updates the state of any associated OrchestratedClusterJob objects.
    :param job_id: Slurm job ID e.g. 10101 or 10101_1
    :return: state of the job, as one of the string values of SlurmStatus.
    """
    logger = get_run_logger()

    try:
        ocj = OrchestratedClusterJob.objects.get(job_id=job_id)
    except (
        OrchestratedClusterJob.DoesNotExist,
        OrchestratedClusterJob.MultipleObjectsReturned,
    ) as e:
        logger.warning(
            f"Did not find exactly one OrchestratedClusterJob to match slurm {job_id = }"
        )
        logger.warning(e.message)
        ocj = None

    try:
        job = pyslurm.db.Job(job_id).load(job_id)
    except pyslurm.core.error.RPCError:
        logger.warning(f"Error talking to slurm for job {job_id}")
        if ocj:
            ocj.last_known_state = SlurmStatus.unknown.value
            ocj.state_checked_at = datetime.now()
            ocj.save()
        return SlurmStatus.unknown.value
    logger.info(f"SLURM status of {job_id = } is {job.state}")

    if ocj:
        ocj.last_known_state = job.state
        ocj.state_checked_at = datetime.now()

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
                ).replace("<<LOG>>", safe(log))
            )

            if ocj:
                ocj.cluster_log = log
    else:
        logger.info(f"No Slurm Job Stdout available at {job_log_path}")
    ocj.save()
    return job.state


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
    logger.info(f"Will run the script ```{safe(script)}```")

    job_description_params = OrchestratedClusterJob.SlurmJobSubmitDescription(
        name=name,
        time_limit=slurm_timedelta(expected_time),
        memory_per_node=memory,
        script=script,
        working_directory=str(job_workdir),
        **kwargs,
    )

    desc = pyslurm.JobSubmitDescription(**job_description_params.model_dump(), **kwargs)
    job_id = desc.submit()
    logger.info(f"Submitted as slurm job {job_id}")

    nf_link = maybe_get_nextflow_tower_browse_url(command)
    nf_link_markdown = f"[Watch Nextflow Workflow]({nf_link})" if nf_link else ""

    ocj = OrchestratedClusterJob.objects.create(
        cluster_job_id=job_id,
        flow_run_id=flow_run.id,
        job_submit_description=job_description_params,
        # input_files_hashes=
    )

    create_markdown_artifact(
        key="slurm-job-submission",
        markdown=_(
            f"""\
            # Slurm job {job_id}
            [Orchestrated Cluster Job {ocj.id}]({reverse("admin:workflows_orchestratedclusterjob_change", kwargs={"id": ocj.id})})
            Submitted a script to Slurm cluster:
            ~~~
            <<SCRIPT>>
            ~~~
            It will be terminated by Slurm if not done in {slurm_timedelta(expected_time)}.
            Slurm working dir is {job_workdir}.
            {nf_link_markdown}
            """
        ).replace("<<SCRIPT>>", safe(script)),
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
    resubmit_policy: Optional[Type[_SlurmResubmitPolicy]] = None,
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
    space_on_cluster = delay_until_cluster_has_space()

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
