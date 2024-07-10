import os
import pytest
from unittest.mock import patch
import asyncio
import analyses.models as mg_models
import ena.models as ena_models
from workflows.flows.assembly_uploader import (
    assembly_uploader
)


@pytest.mark.django_db
@pytest.mark.asyncio
@patch("workflows.flows.assembly_uploader.create_study_xml")
@patch("workflows.flows.assembly_uploader.submit_study_xml")
@patch("workflows.flows.assembly_uploader.generate_assembly_xml")
#@patch("workflows.prefect_utils.slurm_flow.start_cluster_job")
async def test_prefect_assembly_upload_flow(
        mock_generate_assembly_xml,
        mock_submit_study_xml,
        mock_create_study_xml,
        mock_start_cluster_job,
        #mock_check_cluster_job_all_completed,
        prefect_harness,
        capsys):
    """
    This test mocks all assembly_uploader functions and just checks steps execution
    """
    async def mock_create_study_xml_func(*args, **kwargs):
        await asyncio.sleep(1)  # Simulate asynchronous work
        upload_dir = "slurm/fs/hps/tests/assembly_uploader/PRJ1_upload"
        if not os.path.exists(upload_dir):
            os.mkdir(upload_dir)
        return upload_dir
    async def mock_submit_study_xml_func(*args, **kwargs):
        await asyncio.sleep(1)
        return "PRJ2"
    async def mock_mock_generate_assembly_xml_func(*args, **kwargs):
        await asyncio.sleep(1)
        run_manifest = "slurm/fs/hps/tests/assembly_uploader/PRJ1_upload/ERR1.manifest"
        if not os.path.exists(run_manifest):
            os.mknod(run_manifest)
        return True

    mock_create_study_xml.side_effect = mock_create_study_xml_func
    mock_submit_study_xml.side_effect = mock_submit_study_xml_func
    mock_generate_assembly_xml.side_effect = mock_mock_generate_assembly_xml_func

    #mock_check_cluster_job_all_completed.assert_called()

    study_accession = "PRJ1"
    run_accession = "ERR1"
    sample_accession = "SAMP1"
    assembler_name = "metaspades"
    assembler_version = "3.15.3"

    # create DB records for tests
    # TODO: move to fixtures
    ena_study = await ena_models.Study.objects.acreate(accession=study_accession, title="Project 1")
    ena_sample = await ena_models.Sample.objects.acreate(study=ena_study,
                                                        metadata={"accession": sample_accession,
                                                                  "description": "Sample 1"})

    mgnify_study = await mg_models.Study.objects.acreate(ena_study=ena_study, title="Project 1", )
    mgnify_sample = await mg_models.Sample.objects.acreate(ena_sample=ena_sample, ena_study=ena_sample.study)
    mgnify_run = await mg_models.Run.objects.acreate(ena_accessions=[run_accession], study=mgnify_study,
                                              ena_study=mgnify_sample.ena_study, sample=mgnify_sample,
                                                     experiment_type="Metagenomic")
    assembler = await mg_models.Assembler.objects.acreate(name=assembler_name, version=assembler_version)
    mgnify_assembly = await mg_models.Assembly.objects.acreate(run=mgnify_run, reads_study=mgnify_study,
                                                        ena_study=mgnify_run.ena_study,
                                                        assembler=assembler,
                                                        dir='slurm/fs/hps/tests/assembly_uploader',
                                                        metadata={"coverage": 20},
                                                        status={"status": "assembly_completed"})

    await assembly_uploader(study_accession=study_accession, run_accession=run_accession, assembler=assembler_name,
                            assembler_version=assembler_version, dry_run=True)
    captured_logging = capsys.readouterr().err
    # sanity check
    assert "Assembly for ERR1 passed sanity check" in captured_logging
    assert "WARN: ERR1.assembly_graph.fastg.gz does not exist" in captured_logging
    assert "params.txt does not exist" not in captured_logging
    # create study XML
    assert "Upload folder slurm/fs/hps/tests/assembly_uploader/PRJ1_upload and study XMLs were created" in captured_logging
    # submit study
    assert "Study submitted successfully under PRJ2" in captured_logging
    # assembly manifest
    assert os.path.exists("slurm/fs/hps/tests/assembly_uploader/PRJ1_upload/ERR1.manifest")
    # webin-cli cluster job
    mock_start_cluster_job.assert_called()

# TODO test with only Shell mock
# TODO add test for 2 assemblies and no study registration