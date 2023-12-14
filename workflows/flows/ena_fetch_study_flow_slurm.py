from asyncio import run
from datetime import timedelta

import django
from django.conf import settings

from workflows.prefect_utils.slurm_flow import (
    submit_cluster_jobs,
    after_resumption,
    check_or_repause,
)

django.setup()

import httpx

import ena.models
from prefect import flow, task


@task(
    retries=2,
    persist_result=True,
    task_run_name="Get study from ENA: {accession}",
    log_prints=True,
)
def get_study_from_ena(accession: str) -> ena.models.Study:
    print(f"Will fetch from ENA Portal API Study {accession}")
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=accession%3D{accession}&limit=10&format=json&fields=study_title"
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
    task_run_name="Get study samples from ENA: {accession}",
    log_prints=True,
)
def get_study_samples_from_ena(accession: str, limit: int = 10) -> [str]:
    print(f"Will fetch study {accession} samples from ENA portal API")
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
def ena_fetch_study_flow_slurm(accession: str = "PRJEB65441"):
    """
    Get a study and all of its samples from the ENA API, and store in db.
    :param accession: Study accession e.g. PRJxxxxxx
    """
    study = get_study_from_ena(accession)
    print(f"Made study {study}")
    samples = get_study_samples_from_ena(accession, limit=2)
    print(f"Need to get study {accession}")
    study = ena.models.Study.objects.get(accession=accession)
    samples_count = study.samples.count()
    print(f"{study = } has {samples_count} samples")

    job_ids = submit_cluster_jobs(
        name_pattern="Get read runs for {sample}",
        command_pattern=f"nextflow run {settings.EMG_CONFIG.slurm.pipelines_root_dir}/download_read_runs.nf --sample={{sample}}",
        jobs_args=[{"sample": sample} for sample in samples],
        expected_time=timedelta(seconds=10),
        status_checks_limit=1,
        memory="100M",
    )

    finished = run(check_or_repause(job_ids, timedelta(seconds=10)))
    print(f"{finished = }")

    after_resumption(str(job_ids))
