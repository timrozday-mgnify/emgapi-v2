import json

import pytest
from prefect.artifacts import Artifact

from emgapiv2.settings import EMG_CONFIG

import analyses.models
import ena.models
from workflows.flows.analysis_amplicon_study import analysis_amplicon_study


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_prefect_analyse_amplicon_flow(
    prefect_harness,
    httpx_mock,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    raw_read_run,
    raw_read_ena_study
):
    """
    Test should create/get ENA and MGnify study into DB.
    Create analysis for amplicon run and launch it with samplesheet.
    """
    study_accession = "PRJNA398089"
    amplicon_run = "SRR6704248"

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
            f"result=read_run"
            f"&query=%22(study_accession={study_accession}%20OR%20secondary_study_accession={study_accession})%20AND%20library_strategy=AMPLICON%22"
            f"&limit=5000"
            f"&format=json"
            f"&fields={','.join(EMG_CONFIG.ena.readrun_metadata_fields)}"
            f"&dataPortal=metagenome",
        json=[
            {
                "sample_accession": "SAMN08514017",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514017",
                "run_accession": amplicon_run,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{amplicon_run}/{amplicon_run}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{amplicon_run}/{amplicon_run}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome"
            },
        ],
    )

    await analysis_amplicon_study(study_accession=study_accession)

    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()

    assembly_samplesheet_table = await Artifact.get("amplicon-v6-initial-sample-sheet")
    assert assembly_samplesheet_table.type == "table"
    table_data = json.loads(assembly_samplesheet_table.data)
    assert len(table_data) == 1

    assert (
        await analyses.models.Analysis.objects.filter(
            run__ena_accessions__contains=amplicon_run
        ).acount()
        == 1
    )

    assert (
        await analyses.models.Analysis.objects.filter(
            status__analysis_completed=True
        ).acount()
        == 1
    )
