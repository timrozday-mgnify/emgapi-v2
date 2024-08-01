import csv
import os
import tempfile

import pytest
from django.core.management import call_command

from .models import Study, Biome, ComputeResourceHeuristic, Assembler
from ena.models import Study as ENAStudy


@pytest.mark.django_db
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


@pytest.mark.django_db
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


@pytest.mark.django_db
def test_study_biome_lookups(top_level_biomes, mgnify_study):
    cont_cult: Biome = Biome.objects.create(
        biome_name="Continuous culture",
        path="root.engineered.bioreactor.continuous_culture",
    )
    mgnify_study.biome = cont_cult
    mgnify_study.save()

    # something like a lineage lookup:
    eng = Biome.objects.get(path=Biome.lineage_to_path("root:Engineered"))

    # find studies within lineage. this lookup is for studies with a biome whose path is IN the descendants of engineered
    # (slightly counterintuitive naming)
    eng_studies = Study.objects.filter(biome__path__descendants=eng.path)
    assert eng_studies.count() == 1
    assert eng_studies.first() == mgnify_study


@pytest.mark.django_db
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


@pytest.mark.django_db
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
