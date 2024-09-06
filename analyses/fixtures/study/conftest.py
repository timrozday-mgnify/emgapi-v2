import django
import pytest

django.setup()

import analyses.models as mg_models


@pytest.fixture
def raw_reads_mgnify_study(raw_read_ena_study):
    return mg_models.Study.objects.get_or_create(
        ena_study=raw_read_ena_study, title=raw_read_ena_study.title
    )[0]
