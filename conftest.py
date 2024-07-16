import pytest
from unittest.mock import patch, AsyncMock
from prefect.testing.utilities import prefect_test_harness

import analyses.models as mg_models
import ena.models as ena_models
from workflows.prefect_utils.slurm_flow import SlurmStatus


@pytest.fixture
def mgnify_study():
    ena_study = ena_models.Study.objects.create(accession="PRJ1", title="Project 1")
    return mg_models.Study.objects.create(ena_study=ena_study, title="Project 1")


@pytest.fixture
def prefect_harness():
    with prefect_test_harness():
        yield


@pytest.fixture
def mock_suspend_flow_run(request):
    namespace = request.param
    with patch(f"{namespace}.suspend_flow_run", new_callable=AsyncMock) as mock_suspend:
        yield mock_suspend


@pytest.fixture
def mock_cluster_can_accept_jobs_yes():
    with patch(
        "workflows.prefect_utils.slurm_flow.cluster_can_accept_jobs"
    ) as mock_cluster_can_accept_jobs:
        mock_cluster_can_accept_jobs.return_value = 1000
        yield mock_cluster_can_accept_jobs


@pytest.fixture
def mock_start_cluster_job():
    with patch(
        "workflows.prefect_utils.slurm_flow.start_cluster_job"
    ) as mock_start_cluster_job:
        mock_start_cluster_job.side_effect = range(
            1, 1000
        )  # incrementing mocked slurm job ids
        yield mock_start_cluster_job


@pytest.fixture
def mock_check_cluster_job_all_completed():
    with patch(
        "workflows.prefect_utils.slurm_flow.check_cluster_job"
    ) as mock_check_cluster_job:
        mock_check_cluster_job.return_value = SlurmStatus.completed.value
        yield mock_check_cluster_job
