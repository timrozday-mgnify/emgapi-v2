from datetime import timedelta

import django
from asgiref.sync import sync_to_async
from django.conf import settings

from workflows.prefect_utils.slurm_flow import (
    run_cluster_jobs,
    after_cluster_jobs,
)

django.setup()

import httpx

import ena.models
import analyses.models
from prefect import flow, task


@task(
    retries=2,
    persist_result=True,
    task_run_name="Get study from ENA: {accession}",
    log_prints=True,
)
def get_study_from_ena(accession: str) -> ena.models.Study:
    if ena.models.Study.objects.filter(accession=accession).exists():
        return ena.models.Study.objects.get(accession=accession)
    print(f"Will fetch from ENA Portal API Study {accession}")
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=study_accession%3D{accession}%20OR%20secondary_study_accession%3D{accession}&limit=10&format=json&fields=study_title"
    )
    if portal.status_code == httpx.codes.OK:
        study, created = ena.models.Study.objects.get_or_create(
            accession=accession, title=portal.json()[0]["study_title"]
        )
        return study
    else:
        print(f"Bad status! {portal.status_code} {portal}")


@task(
    retries=2,
    persist_result=True,
    task_run_name="Set up MGnify Study: {ena_accession}",
    log_prints=True,
)
def get_mgnify_study(ena_accession: str) -> analyses.models.Study:
    print(f"Will get/create MGnify study for {ena_accession}")
    ena_study = ena.models.Study.objects.get(accession=ena_accession)
    study, _ = analyses.models.Study.objects.get_or_create(
        ena_study=ena_study, title=ena_study.title
    )
    return study


@task(persist_result=True, log_prints=True)
def update_assembly_status(request: analyses.models.AssemblyAnalysisRequest):
    print(f"Analysis Request {request} is now assembled")
    request.status[request.AssemblyAnalysisStates.ASSEMBLY_COMPLETED] = True


@flow(
    name="Assemble and analyse a study",
    log_prints=True,
    flow_run_name="Assemble and analyse: {accession}",
)
async def assembly_analysis_request(request_id: int, accession: str):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off assembly pipeline.
    TODO: analysis pipeline.
    :param accession: Study accession e.g. PRJxxxxxx
    :param request_id: ID of the request in EMG DB.
    """
    request = await sync_to_async(analyses.models.AssemblyAnalysisRequest.objects.get)(
        id=request_id
    )
    ena_study = get_study_from_ena(request.requested_study)
    print(f"ENA Study is {ena_study.accession}: {ena_study.title}")
    mgnify_study = get_mgnify_study(request.requested_study)

    request.status[request.AssemblyAnalysisStates.ASSEMBLY_STARTED] = True
    await run_cluster_jobs(
        name_pattern="Assemble study {study}",
        command_pattern=f"nextflow run {settings.EMG_CONFIG.slurm.pipelines_root_dir}/miassembler/main.nf "
        f"-profile codon_slurm "
        f"-resume "
        f"--assembler metaspades/megahit "
        f"--outdir {{study}}_miassembler "
        f"--study_accession {{study}} "
        f"--reads_accession SRR6180434 ",  # TODO: remove
        jobs_args=[{"study": ena_study.accession}],
        expected_time=timedelta(days=1),
        memory="8G",
    )

    after_cluster_jobs()
    update_assembly_status()
