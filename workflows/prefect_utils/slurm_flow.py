import datetime
import logging
from datetime import timedelta
from enum import Enum
from textwrap import dedent as _
from typing import Union

from prefect import task, flow
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run

from emgapiv2.settings import EMG_CONFIG

try:
    import pyslurm
except:
    logging.warning("No PySlurm available. Patching.")
    import workflows.prefect_utils.pyslurm_patch as pyslurm


class SlurmStatus(str, Enum):
    pending = "PENDING"
    running = "RUNNING"
    completing = "COMPLETING"
    completed = "COMPLETED"
    failed = "FAILED"
    terminated = "TERMINATED"
    suspended = "SUSPENDED"
    stopped = "STOPPED"


def slurm_timedelta(delta: timedelta) -> str:
    t_minutes, seconds = divmod(delta.seconds, 60)
    hours, minutes = divmod(t_minutes, 60)
    days = delta.days
    return f"{days:02}-{hours:02}:{minutes:02}:{seconds:02}"


@task(persist_result=True, task_run_name="Submit SLURM job: {name}")
async def submit_cluster_job(
    name: str, command: str, expected_time: timedelta, memory: Union[int, str], **kwargs
):
    script = _(
        f"""#!/bin/bash
    {command}
    """
    )
    print(f"Will run the script ```{script}```")
    desc = pyslurm.JobSubmitDescription(
        name=name,
        time_limit=slurm_timedelta(expected_time),
        memory_per_node=memory,
        script=script,
        working_directory=EMG_CONFIG.slurm.default_workdir,
        **kwargs,
    )
    return desc.submit(), script


def _get_poll_interval() -> datetime.timedelta:
    parameters = flow_run.parameters
    return timedelta(seconds=parameters["poll_interval_seconds"])


def _get_retries_limit() -> int:
    poll_interval = _get_poll_interval()
    parameters = flow_run.parameters
    return parameters["expected_time"] // poll_interval


@task(
    task_run_name="Check completion of SLURM job: {job_id}",
    retries=10,
    retry_delay_seconds=5,
)
async def assert_cluster_job_completed(job_id: int):
    job = pyslurm.db.Job(job_id).load(job_id)
    print(f"SLURM status of {job_id = } is {job.state}")
    assert job.state == SlurmStatus.completed


@flow(flow_run_name="Job: {name}")
async def await_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    poll_interval_seconds: int = EMG_CONFIG.slurm.default_polling_interval_seconds,
    **kwargs,
):
    poll_interval = timedelta(seconds=poll_interval_seconds)
    max_checks: int = expected_time // poll_interval

    job_id, script = await submit_cluster_job(
        name, command, expected_time, memory, **kwargs
    )
    await create_markdown_artifact(
        key="slurm-job-submission",
        markdown=_(
            f"""
            # Slurm job {job_id}
            Submitted a script to Slurm cluster:
            ~~~
            {script}
            ~~~
            Will check on this job every {poll_interval_seconds} seconds up to {max_checks} times,
            since it will be terminated by Slurm in {slurm_timedelta(expected_time)}.
            """
        ),
    )
    assert_cluster_job_completed(job_id)
