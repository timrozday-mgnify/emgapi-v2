import logging
from datetime import timedelta, datetime
from enum import Enum
from pathlib import Path
from textwrap import dedent as _, indent
from typing import Union, List, Dict

from prefect import task, flow, get_client, get_run_logger
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName
from prefect.runtime import flow_run
from prefect.server.schemas.responses import SetStateStatus, DeploymentResponse
from prefect.states import Paused, Scheduled

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


def slurm_status_is_okay(state: SlurmStatus):
    return state in [
        SlurmStatus.pending,
        SlurmStatus.running,
        SlurmStatus.unknown,
        SlurmStatus.completed,
    ]


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


@task(task_run_name="Job submission: {name}", log_prints=True, persist_result=True)
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


async def _get_resumer_deployment() -> DeploymentResponse:
    logger = get_run_logger()
    async with get_client() as client:
        resumer_deployments = await client.read_deployments(
            deployment_filter=DeploymentFilter(
                name=DeploymentFilterName(like_="resume_flow_run_deployment")
            )
        )
        logger.info(f"Got deployments: {resumer_deployments}")
        if resumer_deployments:
            return resumer_deployments[0]


async def _pause_parent_flow(expected_time: timedelta):
    logger = get_run_logger()
    async with get_client() as client:
        parent_flow_run = await client.read_flow_run(flow_run.parent_flow_run_id)

        logger.info(
            "Will now schedule a job for later to resume this flow run’s parent, and then pause the parent flow run."
        )

        sched_state = Scheduled(scheduled_time=datetime.utcnow() + expected_time)

        resumer_deployment = await _get_resumer_deployment()

        resumer_run = await client.create_flow_run_from_deployment(
            deployment_id=resumer_deployment.id,
            parameters={"flow_run_id": parent_flow_run.id},
            state=sched_state,
        )
        logger.info(
            f"The resumer flowrun is called {resumer_run.name}. In state {resumer_run.state}."
        )

        paused_state = Paused(
            timeout_seconds=int(expected_time.total_seconds()) + 600, reschedule=True
        )
        logger.info(f"Will pause flow {parent_flow_run.id} ({parent_flow_run.name})")
        orchestrated_state = await client.set_flow_run_state(
            parent_flow_run.id, paused_state
        )
        logger.debug(orchestrated_state)
        assert orchestrated_state.status == SetStateStatus.ACCEPT


async def _pause_this_flow(expected_time: timedelta):
    logger = get_run_logger()
    async with get_client() as client:
        logger.info(
            "Will now schedule a job for later to resume this flow run, and then pause the flow run."
        )

        sched_state = Scheduled(scheduled_time=datetime.utcnow() + expected_time)

        resumer_deployment = await _get_resumer_deployment()

        resumer_run = await client.create_flow_run_from_deployment(
            deployment_id=resumer_deployment.id,
            parameters={"flow_run_id": flow_run.id},
            state=sched_state,
        )
        logger.info(
            f"The resumer flowrun is called {resumer_run.name}. In state {resumer_run.state}."
        )

        paused_state = Paused(
            timeout_seconds=int(expected_time.total_seconds()) + 600, reschedule=True
        )
        logger.info(f"Will pause flow {flow_run.id} ({flow_run.name})")
        orchestrated_state = await client.set_flow_run_state(flow_run.id, paused_state)
        logger.debug(orchestrated_state)
        assert orchestrated_state.status == SetStateStatus.ACCEPT


def _generate_flow_run_name_from_jobs_pattern():
    params = flow_run.parameters
    return f"Collection of {len(params.get('jobs_args'))} cluster jobs: \"{params.get('name_pattern')}\""


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
    job_ids = [
        start_cluster_job(
            name=name_pattern.format(**job_args),
            command=command_pattern.format(**job_args),
            expected_time=expected_time,
            memory=memory,
            **kwargs,
        )
        for job_args in jobs_args
    ]
    logger.info(f"{len(job_ids)} jobs submitted to cluster")

    await create_table_artifact(
        key="slurm-group-of-jobs-submission",
        table=[
            {
                "Name": name_pattern.format(**job_args),
                "Command": command_pattern.format(**job_args),
                "Slurm Job ID": job_id,
            }
            for job_args, job_id in zip(jobs_args, job_ids)
        ],
        description="Jobs submitted to Slurm",
    )

    await check_or_repause(
        job_ids,
        next_expected_time=timedelta(
            seconds=expected_time.total_seconds() / status_checks_limit
        ),
    )

    return job_ids


@flow(flow_run_name="Resume {flow_run_id}")
async def resume_flow_run(
    flow_run_id: str,
):
    """
    Resume another FlowRun
    :param flow_run_id: UUID of the FlowRun to resume
    :return:
    """
    logger = get_run_logger()
    async with get_client() as client:
        logger.info(f"Will resume flow fun {flow_run_id}")
        orchestrated_state = await client.resume_flow_run(flow_run_id)
        logger.debug(orchestrated_state)
        assert orchestrated_state.status == SetStateStatus.ACCEPT


async def check_or_repause(
    job_ids: List[int], next_expected_time: timedelta = timedelta(seconds=30)
):
    logger = get_run_logger()
    logger.info(f"This is try number {flow_run.run_count}")
    statuses = [check_cluster_job(job_id) for job_id in job_ids]
    logger.info(f"Slurm Job statuses: {statuses}")

    if not all(map(slurm_status_is_okay, statuses)):
        raise ValueError("One or more jobs were in a bad state")

    await create_table_artifact(
        key="slurm-group-of-jobs-submission",
        table=[
            {"Slurm Job ID": job_id, "Status": status}
            for job_id, status in zip(job_ids, statuses)
        ],
        description="Job statuses from Slurm",
    )
    if all([s == SlurmStatus.completed for s in statuses]):
        logger.info(f"All jobs {job_ids} completed.")
        return True
    else:
        await _pause_this_flow(next_expected_time)
        logger.info("Some jobs not finished yet")
