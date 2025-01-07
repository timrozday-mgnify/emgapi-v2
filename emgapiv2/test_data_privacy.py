import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.urls import reverse
from rest_framework import status

from analyses.management.commands.import_v5_analysis import logger
from analyses.models import Run, Analysis, Study
from ena.models import Study as ENAStudy


def create_analysis(is_private=False):
    run = Run.objects.first()
    return Analysis.objects.create(
        study=run.study, run=run, ena_study=run.ena_study, sample=run.sample,
        is_private=is_private
    )

@pytest.mark.django_db(transaction=True)
def test_public_api_studies_endpoint(api_client):
    """Test that public API only returns public studies"""
    public_study = Study.objects.create(title="Public Study", is_private=False)
    Study.objects.create(title="Private Study", is_private=True)

    logger.info("Public study SURFACE  BEING TESTED IN NEW SEPARATE TEST FILE")

    url = reverse("api:list_mgnify_studies")
    response = api_client.get(url)

    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()['items']) == 1  # Only public study

    assert response.json()["items"][0]["accession"] == public_study.accession


@pytest.mark.django_db(transaction=True)
def test_public_api_analyses_endpoint(raw_read_run, api_client):
    """Test that public API only returns public analyses"""
    public_analysis = create_analysis(is_private=False)
    create_analysis(is_private=True)

    url = reverse("api:list_mgnify_analyses")
    response = api_client.get(url)

    assert response.status_code == status.HTTP_200_OK
    # Only public analysis should be returned
    assert len(response.json()['items']) == 1
    assert response.json()['items'][0]["accession"] == public_analysis.accession


@pytest.mark.django_db(transaction=True)
def test_admin_view_studies(admin_client):
    """Test that admin interface shows all studies"""
    public_study = Study.objects.create(title="Public Study", is_private=False)
    private_study = Study.objects.create(title="Private Study", is_private=True)

    url = reverse("admin:analyses_study_changelist")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode("utf-8")
    assert public_study.title in content
    assert private_study.title in content


@pytest.mark.django_db(transaction=True)
def test_admin_view_analyses(raw_read_run, admin_client):
    """Test that admin interface shows all analyses"""
    public_analysis = create_analysis(is_private=False)
    private_analysis = create_analysis(is_private=True)

    url = reverse("admin:analyses_analysis_changelist")
    response = admin_client.get(url)

    assert response.status_code == 200
    content = response.content.decode("utf-8")
    assert public_analysis.accession in content
    assert private_analysis.accession in content


@pytest.mark.django_db(transaction=True)
def test_study_manager_methods():
# def test_manager_methods(raw_read_run):
    """Test various manager methods for privacy handling"""
    ena_study = ENAStudy.objects.create(accession="PRJ1", title="Project 1")
    public_study = Study.objects.create(ena_study=ena_study, title="Public Study", is_private=False)
    private_study = Study.objects.create(title="Private Study", is_private=True)

    assert Study.objects.count() == 1
    assert Study.objects.first() == public_study
    assert Study.all_objects.count() == 2
    private_studies = Study.objects.private_only()
    assert private_studies.count() == 1
    assert private_studies.first() == private_study

@pytest.mark.django_db(transaction=True)
def test_manager_analysis_methods(raw_read_run):
    public_analysis = create_analysis(is_private=False)
    private_analysis = create_analysis(is_private=True)

    assert Analysis.objects.count() == 1
    assert Analysis.objects.first() == public_analysis
    assert Analysis.all_objects.count() == 2
    private_analyses = Analysis.objects.private_only()
    assert private_analyses.count() == 1
    assert private_analyses.first() == private_analysis

    public_analysis.annotations = {"test": "data"}
    public_analysis.save()

    analysis = Analysis.objects_and_annotations.first()
    assert analysis.annotations == {"test": "data"}
    all_analyses = Analysis.all_objects_and_annotations.all()
    assert len(all_analyses) == 2


@pytest.fixture
def api_client():
    from rest_framework.test import APIClient

    return APIClient()


@pytest.fixture
def admin_client():
    from django.test import Client

    admin_user = User.objects.create_superuser(
        username="admin", email="admin@example.com", password="adminpass123"
    )
    client = Client()
    client.force_login(admin_user)
    return client
