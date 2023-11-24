import time

import django
from prefect.artifacts import create_markdown_artifact

django.setup()

import httpx

try:
    import nextflow
except:
    pass

import ena.models
from prefect import flow, task


@task(retries=2)
def get_study_from_ena(accession: str):
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=accession%3D{accession}&limit=10&format=json&fields=study_title"
    )
    if portal.status_code == httpx.codes.OK:
        study = ena.models.Study.objects.get_or_create(
            accession=accession, title=portal.json()[0]["study_title"]
        )


@task(retries=2)
def get_study_samples_from_ena(accession: str) -> [str]:
    study = ena.models.Study.objects.get(accession=accession)
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/links/study?accession={accession}&result=sample&limit=100&format=json"
    )
    if portal.status_code == httpx.codes.OK:
        for sample in portal.json():
            ena.models.Sample.objects.get_or_create(
                accession=sample["sample_accession"], study=study
            )
    return [sample["sample_accession"] for sample in portal.json()]


@task(name="Fetch sample read files from ENA", log_prints=True)
def ena_fetch_sample_reads(accession: str):
    """
    Get all read run FASTQ files for an ENA sample, using the ENA API.
    Uses nextflow.
    :param accession: Sample accession e.g. SAMEAxxxxxxxx
    """
    execution = nextflow.run(
        "workflows/pipelines/download_read_runs.nf", params={"sample": accession}
    )
    create_markdown_artifact(
        key="sample-fetch",
        markdown=f"""
            # Sample {accession}
            Reads downloaded.
            Took {execution.duration}.
            Status {execution.status}.
            """,
    )
    return execution


@flow(name="Fetch Study and Samples from ENA", log_prints=True)
def ena_fetch_study_flow(accession: str):
    """
    Get a study and all of its samples from the ENA API, and store in db.
    :param accession: Study accession e.g. PRJxxxxxx
    """
    get_study_from_ena(accession)
    samples = get_study_samples_from_ena(accession)
    study = ena.models.Study.objects.get(accession=accession)
    print(f"{study = } has {study.samples.count()} samples")
    for sample in samples:
        ena_fetch_sample_reads(sample)
