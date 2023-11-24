import asyncio
from datetime import timedelta
from typing import Optional

import django
from asgiref.sync import sync_to_async
from django.conf import settings
from prefect.artifacts import create_markdown_artifact

from workflows.prefect_utils.slurm_flow import await_cluster_job

django.setup()

import httpx

import ena.models
from prefect import flow, task


@task(retries=2, persist_result=True, task_run_name="Get study from ENA: {accession}")
def get_study_from_ena(accession: str) -> Optional[str]:
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=accession%3D{accession}&limit=10&format=json&fields=study_title"
    )
    if portal.status_code == httpx.codes.OK:
        study: ena.models.Study = ena.models.Study.objects.get_or_create(
            accession=accession, title=portal.json()[0]["study_title"]
        )
        return str(study)


@task(
    retries=2,
    persist_result=True,
    task_run_name="Get study samples from ENA: {accession}",
)
def get_study_samples_from_ena(accession: str, limit: int = 10) -> [str]:
    study = ena.models.Study.objects.get(accession=accession)
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/links/study?accession={accession}&result=sample&limit={limit}&format=json"
    )
    if portal.status_code == httpx.codes.OK:
        for sample in portal.json():
            ena.models.Sample.objects.get_or_create(
                accession=sample["sample_accession"], study=study
            )
    return [sample["sample_accession"] for sample in portal.json()]


@flow(
    name="Fetch Study and Samples from ENA (Slurm)",
    log_prints=True,
    flow_run_name="Fetch study's samples and reads from ENA: {accession}",
)
async def ena_fetch_study_flow_slurm(accession: str):
    """
    Get a study and all of its samples from the ENA API, and store in db.
    :param accession: Study accession e.g. PRJxxxxxx
    """
    get_study_from_ena(accession)
    samples = get_study_samples_from_ena(accession)
    study = await sync_to_async(ena.models.Study.objects.get)(accession=accession)
    samples_count = await sync_to_async(study.samples.count)()
    print(f"{study = } has {samples_count} samples")
    sample_read_getting_jobs = []
    for sample in samples:
        nf_command = f"nextflow run {settings.EMG_CONFIG.slurm.pipelines_root_dir}/download_read_runs.nf --sample={sample}"
        j = await_cluster_job(
            name=f"Get read runs for {sample}",
            command=nf_command,
            expected_time=timedelta(minutes=5),
            memory="100M",
        )
        sample_read_getting_jobs.append(j)
    await asyncio.gather(*sample_read_getting_jobs)
