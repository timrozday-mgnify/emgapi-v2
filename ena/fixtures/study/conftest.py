import django
import pytest

django.setup()

import ena.models as ena_models


@pytest.fixture
def raw_read_ena_study():
    raw_reads_study = {"primary_accession": "PRJNA398089", "title": "Project 1"}
    return ena_models.Study.objects.get_or_create(
        accession=raw_reads_study["primary_accession"], title=raw_reads_study["title"]
    )[0]
