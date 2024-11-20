import json
import os
import shutil
from enum import Enum

import pytest
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models
import ena.models
from emgapiv2.settings import EMG_CONFIG
from workflows.flows.assemble_study import AssemblerChoices, assemble_study
from workflows.prefect_utils.analyses_models_helpers import task_mark_assembly_status


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
        url=f"https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&dataPortal=metagenome&format=json&fields=sample_accession,sample_title,secondary_sample_accession,fastq_md5,fastq_ftp,library_layout,library_strategy&query=%22(study_accession=PRJNA1%20OR%20secondary_study_accession=PRJNA1)%22&limit=5000",
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

    class BiomeAndAssemblerInputMock(BaseModel):
        biome: BiomeChoices
        assembler: AssemblerChoices

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "BiomeAndAssemblerInput":
            return BiomeAndAssemblerInputMock(
                biome=BiomeChoices["root.engineered"],
                assembler=AssemblerChoices.pipeline_default,
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    assembly_folder = f"{EMG_CONFIG.slurm.default_workdir}/PRJNA1_miassembler"
    os.mkdir(assembly_folder)

    with open(f"{assembly_folder}/assembled_runs.csv", "w") as file:
        file.write("SRR1,metaspades,3.15.5")

    with open(f"{assembly_folder}/qc_failed_runs.csv", "w") as file:
        file.write("SRR2,filter_ratio_threshold_exceeded")

    os.makedirs(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/",
        exist_ok=True,
    )
    with open(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/SRR1_coverage.json",
        "w",
    ) as file:
        json.dump({"coverage": 0.04760503915318373, "coverage_depth": 273.694}, file)

    ### RUN WORKFLOW ###
    await assemble_study(accession, upload=False)

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

    assembly = await analyses.models.Assembly.objects.filter(
        run__ena_accessions__contains="SRR1"
    ).afirst()
    assert 0.0475 < assembly.metadata.get("coverage") < 0.0477

    assert assembly.dir == f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1"

    assert (
        await analyses.models.Assembly.objects.filter(
            status__assembly_completed=True
        ).acount()
        == 1
    )

    failed_assembly: analyses.models.Assembly = (
        await analyses.models.Assembly.objects.aget(status__pre_assembly_qc_failed=True)
    )

    assert (
        failed_assembly.status["pre_assembly_qc_failed_reason"]
        == "filter_ratio_threshold_exceeded"
    )

    shutil.rmtree(assembly_folder, ignore_errors=True)


@pytest.mark.django_db(transaction=True)
def test_assembly_statuses(prefect_harness, mgnify_assemblies):
    assembly = mgnify_assemblies[0]
    assert not any(assembly.status.values())

    # marking as failed should work
    task_mark_assembly_status(
        assembly, assembly.AssemblyStates.ASSEMBLY_FAILED, reason="It broke"
    )
    assembly.refresh_from_db()
    assert assembly.status[assembly.AssemblyStates.ASSEMBLY_FAILED]

    # making as complete later (perhaps a retry) should work, and can unset failed at same time
    task_mark_assembly_status(
        assembly,
        assembly.AssemblyStates.ASSEMBLY_COMPLETED,
        unset_statuses=[
            assembly.AssemblyStates.ASSEMBLY_FAILED,
            assembly.AssemblyStates.ASSEMBLY_BLOCKED,
        ],
    )
    assembly.refresh_from_db()

    assert not assembly.status[assembly.AssemblyStates.ASSEMBLY_FAILED]
    assert not assembly.status[assembly.AssemblyStates.ASSEMBLY_BLOCKED]
    assert assembly.status[assembly.AssemblyStates.ASSEMBLY_COMPLETED]

    # reason should have been updated too
    assert (
        assembly.status["assembly_failed_reason"]
        == "Explicitly unset when setting assembly_completed"
    )

    # reason should not have been updated for a status that was not previously set
    assert "assembly_blocked_reason" not in assembly.status
