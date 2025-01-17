import time
from unittest.mock import MagicMock, patch

import pytest
from prefect import task

from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.slurm_status import SlurmStatus


@pytest.fixture
def mock_cluster_can_accept_jobs_yes():
    with patch(
        "workflows.prefect_utils.slurm_limits.cluster_can_accept_jobs"
    ) as mock_cluster_can_accept_jobs:
        mock_cluster_can_accept_jobs.return_value = 1000
        yield mock_cluster_can_accept_jobs


@task(
    task_run_name="Dummy job submission",
    persist_result=True,
    cache_key_fn=context_agnostic_task_input_hash,
)
def _dummy_start_cluster_job(*args, **kwargs):
    return int(time.time() * 1000)


@pytest.fixture
def mock_start_cluster_job():
    with patch(
        "workflows.prefect_utils.slurm_flow.start_or_attach_cluster_job",
    ) as mock_start_cluster_job_task:
        mock_start_cluster_job_task.side_effect = MagicMock(
            wraps=_dummy_start_cluster_job
        )
        yield mock_start_cluster_job_task


@pytest.fixture
def mock_check_cluster_job_all_completed():
    with patch(
        "workflows.prefect_utils.slurm_flow.check_cluster_job"
    ) as mock_check_cluster_job:
        mock_check_cluster_job.return_value = SlurmStatus.completed.value
        yield mock_check_cluster_job
