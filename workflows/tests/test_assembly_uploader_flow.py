import os
import shutil
from unittest.mock import patch

import pytest

import analyses.models as mg_models
from workflows.flows.assembly_uploader import assembly_uploader
from workflows.prefect_utils.testing_utils import (
    run_async_flow_and_capture_logs,
    run_flow_and_capture_logs,
)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
@patch("workflows.flows.assembly_uploader.create_study_xml")
@patch("workflows.flows.assembly_uploader.submit_study_xml")
@patch("workflows.flows.assembly_uploader.generate_assembly_xml")
@patch("workflows.flows.assembly_uploader.get_assigned_assembly_accession")
async def test_prefect_assembly_upload_flow_assembly_metaspades(
    mock_get_assigned_assembly_accession,
    mock_generate_assembly_xml,
    mock_submit_study_xml,
    mock_create_study_xml,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    raw_read_ena_study,
    raw_reads_mgnify_study,
    raw_read_run,
    mgnify_assembly_completed,
    assemblers,
):
    """
    This test mocks all assembly_uploader functions and just checks steps execution.
    Flow is running 1 metaspades assembly upload
    """

    erz_assigned_accession = "ERZ24815338"
    study_accession = raw_read_ena_study.accession
    run_accession = "SRR6180434"

    registered_study = "PRJNA567089"

    assembler_name = "metaspades"
    assembler_version = "3.15.3"

    # FS required bits
    upload_dir = (
        f"slurm-dev-environment/fs/hps/tests/assembly_uploader/{study_accession}_upload"
    )
    reg_xml = f"slurm-dev-environment/fs/hps/tests/assembly_uploader/{study_accession}_upload/reg.xml"
    submission_xml = f"slurm-dev-environment/fs/hps/tests/assembly_uploader/{study_accession}_upload/submission.xml"

    async def mock_create_study_xml_func(*args, **kwargs):
        if not os.path.exists(upload_dir):
            os.mkdir(upload_dir)
            os.mknod(reg_xml)
            os.mknod(submission_xml)

    async def mock_submit_study_xml_func(*args, **kwargs):
        return registered_study

    async def mock_mock_generate_assembly_xml_func(*args, **kwargs):
        run_manifest = f"slurm-dev-environment/fs/hps/tests/assembly_uploader/{study_accession}_upload/{run_accession}.manifest"
        if not os.path.exists(run_manifest):
            os.mknod(run_manifest)
        return True

    async def mock_get_assigned_assembly_accession_func(*args, **kwargs):
        return erz_assigned_accession

    mock_create_study_xml.side_effect = mock_create_study_xml_func
    mock_submit_study_xml.side_effect = mock_submit_study_xml_func
    mock_generate_assembly_xml.side_effect = mock_mock_generate_assembly_xml_func
    mock_get_assigned_assembly_accession.side_effect = (
        mock_get_assigned_assembly_accession_func
    )

    logged_uploader_result = await run_async_flow_and_capture_logs(
        assembly_uploader,
        study_accession=study_accession,
        run_accession=run_accession,
        assembler=assembler_name,
        assembler_version=assembler_version,
        dry_run=True,
    )

    captured_logging = logged_uploader_result.logs

    # sanity check
    assert f"Assembly for {run_accession} passed sanity check" in captured_logging
    assert f"{run_accession}.assembly_graph.fastg.gz does not exist" in captured_logging
    assert "params.txt does not exist" not in captured_logging
    # create study XML
    assert os.path.exists(
        f"slurm-dev-environment/fs/hps/tests/assembly_uploader/{study_accession}_upload"
    )
    # submit study
    assert f"Study submitted successfully under {registered_study}" in captured_logging
    # assembly manifest
    assert os.path.exists(
        f"slurm-dev-environment/fs/hps/tests/assembly_uploader/{study_accession}_upload/{run_accession}.manifest"
    )

    # Clean up
    shutil.rmtree(upload_dir, ignore_errors=True)

    # webin-cli cluster job
    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()

    assert (
        await mg_models.Assembly.objects.filter(status__assembly_uploaded=True).acount()
        == 1
    )

    assert (
        await mg_models.Assembly.objects.filter(
            status__assembly_upload_failed=True
        ).acount()
        == 0
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_prefect_assembly_upload_flow_post_assembly_sanity_check_not_passed(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    mgnify_assembly_completed_uploader_sanity_check,
    assemblers,
    raw_read_ena_study,
):
    """
    This test mocks all assembly_uploader functions and just checks steps execution.
    Flow is running 1 metaspades assembly
    """

    assembler_name = "metaspades"
    assembler_version = "3.15.3"

    study_accession = raw_read_ena_study.accession
    run_accession = "SRR6180435"

    with pytest.raises(Exception) as excinfo:
        await assembly_uploader(
            study_accession=study_accession,
            run_accession=run_accession,
            assembler=assembler_name,
            assembler_version=assembler_version,
            dry_run=True,
        )
    assert (
        str(excinfo.value)
        == f"Assembly for {run_accession} did not pass sanity check. No further action."
    )

    assert (
        await mg_models.Assembly.objects.filter(
            status__post_assembly_qc_failed=True
        ).acount()
        == 1
    )

    assert (
        await mg_models.Assembly.objects.filter(
            status__post_assembly_completed=True
        ).acount()
        == 0
    )


# TODO test with only Shell mock
