import pytest
from .models import Study
from ena.models import Study as ENAStudy


@pytest.mark.django_db
def test_study():
    # Test accessioning
    ena_study = ENAStudy.objects.create(accession="PRJ1", title="Project 1")
    study = Study.objects.create(ena_study=ena_study, title="Project 1")
    assert study.accession == "MGYS00000001"
