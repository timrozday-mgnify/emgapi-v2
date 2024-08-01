import json
from enum import Enum

import pytest
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models
import ena.models
from workflows.flows.assemble_study import (
    assemble_study,
    AssemblerInput,
    AssemblerChoices,
)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.assemble_study"], indirect=True
)
async def test_prefect_assemble_study_flow(
    prefect_harness,
    httpx_mock,
    mock_suspend_flow_run,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    top_level_biomes,
    assemblers,
):
    ### ENA MOCKING ###
    accession = "SRP1"

    httpx_mock.add_response(
        url=f"https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=study_accession%3D{accession}%20OR%20secondary_study_accession%3D{accession}&limit=10&format=json&fields=study_title,secondary_study_accession",
        json=[
            {
                "study_title": "Metagenome of a wookie",
                "secondary_study_accession": "SRP1",
                "study_accession": "PRJNA1",
            }
        ],
    )

    httpx_mock.add_response(
        url=f'https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&dataPortal=metagenome&format=json&fields=sample_accession,sample_title,secondary_sample_accession,fastq_md5,fastq_ftp,library_layout,library_strategy&query="study_accession=PRJNA1 OR secondary_study_accession=PRJNA1"&limit=5000&format=json',
        json=[
            {
                "sample_accession": "SAMN01",
                "sample_title": "Wookie hair 1",
                "secondary_sample_accession": "SRS1",
                "run_accession": "SRR1",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR1/SRR1_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR1/SRR1_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
            },
            {
                "sample_accession": "SAMN02",
                "sample_title": "Wookie hair 2",
                "secondary_sample_accession": "SRS2",
                "run_accession": "SRR2",
                "fastq_md5": "456;xyz",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR2/SRR2_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR2/SRR2_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
            },
        ],
    )

    ## Pretend that a human resumed the flow with the biome picker, and then with the assembler selector.
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})

    class BiomeInputMock(BaseModel):
        biome: BiomeChoices

    class AssemblerInputMock(BaseModel):
        assembler: AssemblerChoices
        memory_gb: int

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "BiomeInput":
            return BiomeInputMock(biome=BiomeChoices["root.engineered"])
        if wait_for_input.__name__ == "AssemblerInput":
            return AssemblerInputMock(
                assembler=AssemblerChoices.pipeline_default, memory_gb=8
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect
    ## ----

    ### RUN WORKFLOW ###
    await assemble_study(accession)

    ### MOCKS WERE ALL CALLED ###
    mock_suspend_flow_run.assert_called()
    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()

    ### CLUSTER JOBS WERE SUBMITTED AND LOGGED AS EXPECTED ###
    assembly_samplesheet_table = await Artifact.get("miassembler-initial-sample-sheet")
    assert assembly_samplesheet_table.type == "table"
    table_data = json.loads(assembly_samplesheet_table.data)
    assert len(table_data) == 2

    ### DB OBJECTS WERE CREATED AS EXPECTED ###
    assert await ena.models.Study.objects.acount() == 1
    assert await analyses.models.Study.objects.acount() == 1
    mgys = await analyses.models.Study.objects.select_related("ena_study").afirst()
    assert mgys.ena_study == await ena.models.Study.objects.afirst()

    assert await ena.models.Sample.objects.acount() == 2
    assert await analyses.models.Sample.objects.acount() == 2
    assert await analyses.models.Run.objects.acount() == 2
    assert await analyses.models.Assembly.objects.acount() == 2

    assert (
        await analyses.models.Assembly.objects.filter(
            status__assembly_completed=True
        ).acount()
        == 2
    )
