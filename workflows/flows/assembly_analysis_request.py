from datetime import timedelta

import django
from django.conf import settings
from prefect.input import RunInput
from prefect.task_runners import SequentialTaskRunner

from workflows.prefect_utils.slurm_flow import run_cluster_job

django.setup()

import httpx

import ena.models
import analyses.models
from prefect import flow, task, suspend_flow_run


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
def mark_assembly_as_completed(request: analyses.models.AssemblyAnalysisRequest):
    print(f"Analysis Request {request} is now assembled")
    request.mark_status(request.AssemblyAnalysisStates.ASSEMBLY_COMPLETED)


@task(persist_result=True, log_prints=True)
def mark_assembly_as_started(request: analyses.models.AssemblyAnalysisRequest):
    request.mark_status(request.AssemblyAnalysisStates.ASSEMBLY_STARTED)


class ReadsAccessionInput(RunInput):
    reads_accession: str


@flow(
    name="Assemble and analyse a study",
    log_prints=True,
    flow_run_name="Assemble and analyse: {accession}",
    task_runner=SequentialTaskRunner,
)
async def assembly_analysis_request(request_id: int, accession: str):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off assembly pipeline.
    TODO: analysis pipeline.
    :param accession: Study accession e.g. PRJxxxxxx
    :param request_id: ID of the request in EMG DB.
    """
    request = await analyses.models.AssemblyAnalysisRequest.objects.aget(id=request_id)
    ena_study = get_study_from_ena(request.requested_study)
    print(f"ENA Study is {ena_study.accession}: {ena_study.title}")
    mgnify_study = get_mgnify_study(request.requested_study)
    print(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    reads_accession_input: ReadsAccessionInput = await suspend_flow_run(
        wait_for_input=ReadsAccessionInput
    )
    print(f"Will assemble only reads {reads_accession_input}")

    mark_assembly_as_started(request)

    assembler_job = await run_cluster_job(
        name=f"Assemble study {ena_study.accession}",
        command=f"nextflow run {settings.EMG_CONFIG.slurm.pipelines_root_dir}/miassembler/main.nf "
        f"-profile codon_slurm "
        f"-resume "
        f"--assembler megahit "
        f"--outdir {ena_study.accession}_miassembler "
        f"--study_accession {ena_study.accession} "
        f"--reads_accession {reads_accession_input.reads_accession} ",  # TODO: automate for all
        expected_time=timedelta(days=1),
        memory="8G",
    )

    mark_assembly_as_completed(request, wait_for=assembler_job)
