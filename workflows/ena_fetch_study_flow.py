import time

import django
django.setup()

import httpx

import ena.models
from prefect import flow, task


@task(retries=2)
def get_study_from_ena(accession: str):
    time.sleep(30)  # such a long running process
    portal = httpx.get(f"https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=accession%3D{accession}&limit=10&format=json&fields=study_title")
    if portal.status_code == httpx.codes.OK:
        study = ena.models.Study.objects.get_or_create(accession=accession, title=portal.json()[0]['study_title'])


@task(retries=2)
def get_study_samples_from_ena(accession: str):
    study = ena.models.Study.objects.get(accession=accession)
    portal = httpx.get(f"https://www.ebi.ac.uk/ena/portal/api/links/study?accession={accession}&result=sample&limit=100&format=json")
    if portal.status_code == httpx.codes.OK:
        for sample in portal.json():
            ena.models.Sample.objects.get_or_create(accession=sample['sample_accession'], study=study)


@flow(name="Fetch Study and Samples from ENA", log_prints=True)
def ena_fetch_study_flow(accession: str):
    """
    Get a study and all of its samples from the ENA API, and store in db.
    :param accession: Study accesion eg. PRJxxxxxx
    """
    get_study_from_ena(accession)
    get_study_samples_from_ena(accession)
    study = ena.models.Study.objects.get(accession=accession)
    print(f"{study = } has {study.samples.count()} samples")
