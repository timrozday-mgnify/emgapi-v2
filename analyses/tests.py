import csv
import os
import tempfile

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.urls import reverse
# from rest_framework import status

from ena.models import Study as ENAStudy

from .models import (
    Analysis,
    Assembler,
    Assembly,
    Biome,
    ComputeResourceHeuristic,
    Run,
    Study,
)


@pytest.mark.django_db(transaction=True, reset_sequences=True)
def test_study():
    # Test accessioning
    ena_study = ENAStudy.objects.create(accession="PRJ1", title="Project 1")
    study = Study.objects.create(ena_study=ena_study, title="Project 1")
    assert study.accession == "MGYS00000001"


def test_biome_lineage_path_generator():
    assert Biome.lineage_to_path("root") == "root"
    assert Biome.lineage_to_path("Root") == "root"
    assert (
        Biome.lineage_to_path("root:Host-associated:Human:Digestive system")
        == "root.host_associated.human.digestive_system"
    )
    assert (
        Biome.lineage_to_path("root:Host-associated:Human:Digestive system (bowel)")
        == "root.host_associated.human.digestive_system_bowel"
    )


@pytest.mark.django_db(transaction=True)
def test_biome_hierarchy():
    root: Biome = Biome.objects.create(biome_name="root", path="root")
    engineered: Biome = Biome.objects.create(
        biome_name="Engineered", path="root.engineered"
    )
    wet_ferm: Biome = Biome.objects.create(
        biome_name="Wet fermentation", path="root.engineered.wet_fermentation"
    )
    cont_cult: Biome = Biome.objects.create(
        biome_name="Continuous culture",
        path="root.engineered.bioreactor.continuous_culture",
    )

    # only one root to hierarchy
    assert Biome.objects.roots().count() == 1

    # root has 4 descendants (includes self)
    root.refresh_from_db()
    assert root.descendants_count == 4

    # engineered has 3
    engineered.refresh_from_db()
    assert engineered.descendants_count == 3

    # wet fermentaiton has 1 (self)
    wet_ferm.refresh_from_db()
    assert wet_ferm.descendants_count == 1

    # pretty lineage is colon separated with capitals and spaces
    assert wet_ferm.pretty_lineage == "root:Engineered:Wet fermentation"

    # if a level of hierarchy is skipped (bioreactor) it is simply missing in pretty version (known limitation)
    cont_cult.refresh_from_db()
    assert cont_cult.pretty_lineage == "root:Engineered:Continuous culture"

    # ancestors are as expected (self, engineered, root)
    assert cont_cult.ancestors().count() == 3
    assert cont_cult.ancestors().filter(biome_name="root").exists()
    assert cont_cult.ancestors().filter(biome_name="Engineered").exists()
    assert cont_cult.ancestors().filter(biome_name="Continuous culture").exists()

    # (direct) children should be 1
    assert engineered.children().count() == 1
    # add another child
    Biome.objects.create(biome_name="Bioreactor", path="root.engineered.bioreactor")
    assert engineered.children().count() == 2


@pytest.mark.django_db(transaction=True)
def test_study_biome_lookups(top_level_biomes, raw_reads_mgnify_study):
    cont_cult: Biome = Biome.objects.create(
        biome_name="Continuous culture",
        path="root.engineered.bioreactor.continuous_culture",
    )
    raw_reads_mgnify_study.biome = cont_cult
    raw_reads_mgnify_study.save()

    # something like a lineage lookup:
    eng = Biome.objects.get(path=Biome.lineage_to_path("root:Engineered"))

    # find studies within lineage. this lookup is for studies with a biome whose path is IN the descendants of engineered
    # (slightly counterintuitive naming)
    eng_studies = Study.objects.filter(biome__path__descendants=eng.path)
    assert eng_studies.count() == 1
    assert eng_studies.first() == raw_reads_mgnify_study


@pytest.mark.django_db(transaction=True)
def test_compute_resource_heuristics(top_level_biomes, assemblers):
    # test CSV importer
    with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="") as temp_csv:
        writer = csv.DictWriter(
            temp_csv, fieldnames=["lineage", "memory_gb", "assembler"]
        )
        writer.writeheader()
        writer.writerow(
            {
                "lineage": "root:Host-associated",
                "memory_gb": 100,
                "assembler": "metaspades",
            }
        )
        writer.writerow(
            {
                "lineage": "root:Host-associated:Human",
                "memory_gb": 50,
                "assembler": "metaspades",
            }
        )
        temp_csv_name = temp_csv.name

    try:
        call_command("import_assembler_memory_compute_heuristics", "-p", temp_csv_name)

        assert ComputeResourceHeuristic.objects.count() == 2

        # should select the closest ancestor biome of a deep biome
        deep_biome = Biome.objects.create(
            path=Biome.lineage_to_path("root:Host-associated:Human:Digestive system"),
            biome_name="Digestive system",
        )
        ram_for_deep_biome = (
            ComputeResourceHeuristic.objects.filter(
                process=ComputeResourceHeuristic.ProcessTypes.ASSEMBLY,
                assembler=assemblers.get(name=Assembler.METASPADES),
                biome__path__ancestors=deep_biome.path,
            )
            .reverse()
            .first()
        )
        assert ram_for_deep_biome is not None
        assert ram_for_deep_biome.memory_gb == 50

    finally:
        os.remove(temp_csv_name)


@pytest.mark.django_db(transaction=True)
def test_biome_importer(httpx_mock):
    httpx_mock.add_response(
        url=f"http://old.api/v1/biomes?page=1",
        json={
            "links": {
                "next": "http://old.api/v1/biomes?page=2",
            },
            "data": [{"id": "root", "attributes": {"biome-name": "Root"}}],
        },
    )
    httpx_mock.add_response(
        url=f"http://old.api/v1/biomes?page=2",
        json={
            "links": {
                "next": None,
            },
            "data": [{"id": "root:Deep", "attributes": {"biome-name": "Deep"}}],
        },
    )
    call_command("import_biomes_from_api_v1", "-u", "http://old.api/v1/biomes")
    assert Biome.objects.count() == 2
    assert Biome.objects.filter(path="root.deep").exists()
    assert Biome.objects.get(path="root.deep").biome_name == "Deep"
    assert Biome.objects.get(path="root.deep").pretty_lineage == "root:Deep"


@pytest.mark.django_db(transaction=True)
def test_analysis_inheritance(
    raw_read_run,
):
    for run in Run.objects.all():
        analysis = Analysis.objects.create(
            study=run.study, run=run, ena_study=run.ena_study, sample=run.sample
        )
        analysis.inherit_experiment_type()
        analysis.refresh_from_db()
        assert analysis.experiment_type != analysis.ExperimentTypes.UNKNOWN
        assert analysis.experiment_type == run.experiment_type


########## PRIVACY TESTS##########
########## PRIVACY TESTS ##########

#
# @pytest.mark.django_db(transaction=True)
# def test_public_api_studies_endpoint(api_client):
#     """Test that public API only returns public studies"""
#     public_study = Study.objects.create(title="Public Study", is_private=False)
#     private_study = Study.objects.create(title="Private Study", is_private=True)
#
#     url = reverse("api:list_mgnify_studies")
#     response = api_client.get(url)
#
#     assert response.status_code == status.HTTP_200_OK
#     assert len(response.json()) == 1  # Only public study
#     assert response.json()[0]["accession"] == public_study.accession
#
#
# @pytest.mark.django_db(transaction=True)
# def test_public_api_analyses_endpoint(api_client):
#     """Test that public API only returns public analyses"""
#     public_study = Study.objects.create(title="Public Study", is_private=False)
#     private_study = Study.objects.create(title="Private Study", is_private=True)
#
#     public_analysis = Analysis.objects.create(study=public_study, is_private=False)
#     Analysis.objects.create(study=private_study, is_private=True)  # private analysis
#
#     url = reverse("api:list_mgnify_analyses")
#     response = api_client.get(url)
#
#     assert response.status_code == status.HTTP_200_OK
#     assert len(response.json()) == 1  # Only public analysis
#     assert response.json()[0]["accession"] == public_analysis.accession
#
#
# @pytest.mark.django_db(transaction=True)
# def test_admin_view_studies(admin_client):
#     """Test that admin interface shows all studies"""
#     public_study = Study.objects.create(title="Public Study", is_private=False)
#     private_study = Study.objects.create(title="Private Study", is_private=True)
#
#     url = reverse("admin:analyses_study_changelist")
#     response = admin_client.get(url)
#
#     assert response.status_code == 200
#     content = response.content.decode("utf-8")
#     assert public_study.title in content
#     assert private_study.title in content
#
#
# @pytest.mark.django_db(transaction=True)
# def test_admin_view_analyses(admin_client):
#     """Test that admin interface shows all analyses"""
#     public_study = Study.objects.create(title="Public Study", is_private=False)
#     private_study = Study.objects.create(title="Private Study", is_private=True)
#
#     public_analysis = Analysis.objects.create(study=public_study, is_private=False)
#     private_analysis = Analysis.objects.create(study=private_study, is_private=True)
#
#     url = reverse("admin:analyses_analysis_changelist")
#     response = admin_client.get(url)
#
#     assert response.status_code == 200
#     content = response.content.decode("utf-8")
#     assert public_analysis.accession in content
#     assert private_analysis.accession in content
#
#
# @pytest.mark.django_db(transaction=True)
# def test_manager_methods():
#     """Test various manager methods for privacy handling"""
#     public_study = Study.objects.create(title="Public Study", is_private=False)
#     private_study = Study.objects.create(title="Private Study", is_private=True)
#
#     assert Study.objects.count() == 1
#     assert Study.objects.first() == public_study
#     assert Study.all_objects.count() == 2
#     private_studies = Study.objects.private_only()
#     assert private_studies.count() == 1
#     assert private_studies.first() == private_study
#
#     public_analysis = Analysis.objects.create(study=public_study, is_private=False)
#     private_analysis = Analysis.objects.create(study=private_study, is_private=True)
#
#     assert Analysis.objects.count() == 1
#     assert Analysis.objects.first() == public_analysis
#     assert Analysis.all_objects.count() == 2
#     private_analyses = Analysis.objects.private_only()
#     assert private_analyses.count() == 1
#     assert private_analyses.first() == private_analysis
#
#     public_analysis.annotations = {"test": "data"}
#     public_analysis.save()
#
#     analysis = Analysis.objects_and_annotations.first()
#     assert analysis.annotations == {"test": "data"}
#     all_analyses = Analysis.all_objects_and_annotations.all()
#     assert len(all_analyses) == 2
#
#
# @pytest.fixture
# def api_client():
#     from rest_framework.test import APIClient
#
#     return APIClient()
#
#
# @pytest.fixture
# def admin_client():
#     from django.test import Client
#
#     admin_user = User.objects.create_superuser(
#         username="admin", email="admin@example.com", password="adminpass123"
#     )
#     client = Client()
#     client.force_login(admin_user)
#     return client
