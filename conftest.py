import pytest
from unittest.mock import patch, AsyncMock
from prefect.testing.utilities import prefect_test_harness

import analyses.models as mg_models
import ena.models as ena_models
from workflows.prefect_utils.slurm_flow import SlurmStatus

@pytest.fixture
def ena_study_fixture():
    return ena_models.Study.objects.create(accession="PRJ1", title="Project 1")

# TODO: resolve usage fixtures in tests directly: database locked problem
@pytest.fixture
def ena_sample(ena_study_fixture):
    return ena_models.Sample.objects.create(study=ena_study_fixture, metadata={"accession": "SAMP1", "description": "Sample 1"})

@pytest.fixture
def mgnify_study(ena_study_fixture):
    return mg_models.Study.objects.create(ena_study=ena_study_fixture, title="Project 1")

@pytest.fixture
def mgnify_sample(ena_sample):
    return mg_models.Sample.objects.create(ena_sample=ena_sample, ena_study=ena_sample.study)

@pytest.fixture
def mgnify_run(mgnify_study, mgnify_sample):
    return mg_models.Run.objects.create(ena_accessions=["ERR1"], study=mgnify_study, ena_study=mgnify_sample.ena_study, sample=mgnify_sample)

@pytest.fixture
def mgnify_assembly(mgnify_study, mgnify_run):
    return mg_models.Assembly.objects.create(run=mgnify_run, reads_study=mgnify_study, ena_study=mgnify_run.ena_study)

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
