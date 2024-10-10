import re
from datetime import timedelta

import pytest
from prefect import flow
from prefect.runtime import flow_run
from prefect.variables import Variable

from workflows.prefect_utils.slurm_flow import run_cluster_job
from workflows.prefect_utils.testing_utils import run_async_flow_and_capture_logs


@flow(log_prints=True, retries=2)
async def intermittently_buggy_flow_that_includes_a_cluster_job_subflow():
    print("starting flow")
    job_id = await run_cluster_job(
        name="test job in buggy flow",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )
    print(f"JOB ID = {job_id}")
    if flow_run.run_count == 1:
        raise Exception("Failing first time")

    else:
        print(f"Not failing because on {flow_run.run_count = }")

    return job_id


@pytest.mark.asyncio
async def test_run_cluster_job_state_persistence(
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_check_cluster_job_all_completed,
    tmp_path,
):
    job_id_initial = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )

    # exactly the same inputs should NOT start another cluster job
    # we assume identical calls should not usually start identical another job

    job_id_repeat_call = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )
    assert job_id_initial == job_id_repeat_call  # same job ID as before

    # a change to the params should start a new job
    job_id_altered_call = await run_cluster_job(
        name="test job",
        command="echo 'test but different'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )
    assert (
        int(job_id_altered_call) == int(job_id_initial) + 1
    )  # different job ID to before
    # assumes tests are not being run in parallel :)

    # we can use Variables to do some (clumsy) explicit cache control
    await Variable.set(f"restart_{job_id_initial}", "true")
    job_id_explicitly_resubmitted_call = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
    )
    assert job_id_initial != job_id_explicitly_resubmitted_call  # different job id

    # automatic retries of a buggy flow that fails after a cluster job should not resubmit cluster job
    logged_buggy_flow = await run_async_flow_and_capture_logs(
        intermittently_buggy_flow_that_includes_a_cluster_job_subflow
    )
    assert "Failing first time" in logged_buggy_flow.logs
    assert "Not failing because on flow_run.run_count = 2" in logged_buggy_flow.logs

    job_ids_mentioned = re.findall(r"JOB ID\s*=\s*(\d+)", logged_buggy_flow.logs)
    unique_job_ids = set(job_ids_mentioned)
    assert len(unique_job_ids) == 1
    # (flow ran twice, job started once)

    with open(f"{tmp_path}/my_inputs.csv", "w") as file:
        file.write("my,initial,params")

    # cluster jobs can accept a list of input files to hash
    job_id_initial_with_hash = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        input_files_to_hash=[tmp_path / "my_inputs.csv"],
    )

    # if input file unchanged, should be same job
    job_id_repeat_with_hash = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        input_files_to_hash=[tmp_path / "my_inputs.csv"],
    )
    assert job_id_initial_with_hash == job_id_repeat_with_hash

    # if input file changes, should be new job
    with open(f"{tmp_path}/my_inputs.csv", "w") as file:
        file.write("my,altered,params")

    job_id_repeat_with_hash = await run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        input_files_to_hash=[tmp_path / "my_inputs.csv"],
    )
    assert job_id_initial_with_hash != job_id_repeat_with_hash
