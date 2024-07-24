import os
import pytest
from unittest.mock import patch
import analyses.models as mg_models
import ena.models as ena_models
from workflows.flows.assembly_uploader import assembly_uploader


@pytest.mark.django_db
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
    caplog,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
):
    """
    This test mocks all assembly_uploader functions and just checks steps execution.
    All fixtures are implemented in the test.
    Flow is running 1 metaspades assembly
    """

    study_accession = "PRJNA398089"
    assembly_study_accession = "PRJNA567089"
    run_accession = "SRR6180434"
    erz_assigned_accession = "ERZ24815338"
    sample_accession = "SAMN07793787"
    assembler_name = "metaspades"
    assembler_version = "3.15.3"

    async def mock_create_study_xml_func(*args, **kwargs):
        upload_dir = f"slurm/fs/hps/tests/assembly_uploader/{study_accession}_upload"
        reg_xml = (
            f"slurm/fs/hps/tests/assembly_uploader/{study_accession}_upload/reg.xml"
        )
        submission_xml = f"slurm/fs/hps/tests/assembly_uploader/{study_accession}_upload/submission.xml"
        if not os.path.exists(upload_dir):
            os.mkdir(upload_dir)
            os.mknod(reg_xml)
            os.mknod(submission_xml)

    async def mock_submit_study_xml_func(*args, **kwargs):
        return assembly_study_accession

    async def mock_mock_generate_assembly_xml_func(*args, **kwargs):
        run_manifest = f"slurm/fs/hps/tests/assembly_uploader/{study_accession}_upload/{run_accession}.manifest"
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

    # create DB records for tests
    # TODO: move to fixtures
    ena_study = await ena_models.Study.objects.acreate(
        accession=study_accession, title="Project 1"
    )
    ena_sample = await ena_models.Sample.objects.acreate(
        study=ena_study,
        metadata={"accession": sample_accession, "description": "Sample 1"},
    )

    mgnify_study = await mg_models.Study.objects.acreate(
        ena_study=ena_study,
        title="Project 1",
    )
    mgnify_sample = await mg_models.Sample.objects.acreate(
        ena_sample=ena_sample, ena_study=ena_sample.study
    )
    mgnify_run = await mg_models.Run.objects.acreate(
        ena_accessions=[run_accession],
        study=mgnify_study,
        ena_study=mgnify_sample.ena_study,
        sample=mgnify_sample,
        experiment_type="Metagenomic",
    )
    assembler = await mg_models.Assembler.objects.acreate(
        name=assembler_name, version=assembler_version
    )
    mgnify_assembly = await mg_models.Assembly.objects.acreate(
        run=mgnify_run,
        reads_study=mgnify_study,
        ena_study=mgnify_run.ena_study,
        assembler=assembler,
        dir="slurm/fs/hps/tests/assembly_uploader",
        metadata={"coverage": 20},
        status={"status": "assembly_completed"},
    )

    await assembly_uploader(
        study_accession=study_accession,
        run_accession=run_accession,
        assembler=assembler_name,
        assembler_version=assembler_version,
        dry_run=True,
    )
    captured_logging = caplog.text
    # sanity check
    assert f"Assembly for {run_accession} passed sanity check" in captured_logging
    assert f"{run_accession}.assembly_graph.fastg.gz does not exist" in captured_logging
    assert "params.txt does not exist" not in captured_logging
    # create study XML
    assert os.path.exists(
        f"slurm/fs/hps/tests/assembly_uploader/{study_accession}_upload"
    )
    # submit study
    assert (
        f"Study submitted successfully under {assembly_study_accession}"
        in captured_logging
    )
    # assembly manifest
    assert os.path.exists(
        f"slurm/fs/hps/tests/assembly_uploader/{study_accession}_upload/{run_accession}.manifest"
    )
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


# TODO test with only Shell mock

# TODO fix fixtures usage
"""
async def test_fixtures_usage(prefect_harness, ena_study_fixture):
    study_accession = "PRJ1"
    run_accession = "ERR1"

    await assembly_uploader(study_accession=study_accession, run_accession=run_accession, dry_run=True)
"""
