from datetime import timedelta

import pytest
from prefect import flow
from prefect.runtime import flow_run

from workflows.prefect_utils.slurm_flow import run_cluster_job
from workflows.prefect_utils.testing_utils import run_async_flow_and_capture_logs


@flow(log_prints=True, retries=2)
async def intermittently_buggy_flow_that_includes_a_cluster_job_subflow():
    print("starting flow")
    job_id = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )

    if flow_run.run_count == 1:
        raise Exception("Failing first time")

    else:
        print(f"Not failing because on {flow_run.run_count = }")

    return job_id


@pytest.mark.asyncio
async def test_run_cluster_job_state_persistence(
    prefect_harness,
    mock_start_cluster_job,
    mock_cluster_can_accept_jobs_yes,
    mock_check_cluster_job_all_completed,
):
    job_id_initial = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )
    assert mock_start_cluster_job.call_count == 1

    # exactly the same inputs should still make a new job, because run_cluster_job is flow-aware,
    # i.e. results are not persisted between different flow runs
    job_id_repeat_call = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )
    assert job_id_initial != job_id_repeat_call  # different job ID to before
    assert (
        mock_start_cluster_job.call_count == 2
    )  # start cluster job called an extra time

    # if cluster job subflow is part of a bigger flow,
    # retrying it should use persisted cluster job

    logged_buggy_flow = await run_async_flow_and_capture_logs(
        intermittently_buggy_flow_that_includes_a_cluster_job_subflow
    )
    assert "Failing first time" in logged_buggy_flow.logs
    assert "Not failing because on flow_run.run_count = 2" in logged_buggy_flow.logs
    assert mock_start_cluster_job.call_count == 3
