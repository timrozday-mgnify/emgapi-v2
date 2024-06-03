import pytest
from prefect.testing.utilities import prefect_test_harness

import analyses.models as mg_models
import ena.models as ena_models


@pytest.fixture
def mgnify_study():
    ena_study = ena_models.Study.objects.create(accession="PRJ1", title="Project 1")
    return mg_models.Study.objects.create(ena_study=ena_study, title="Project 1")


@pytest.fixture
def prefect_harness():
    with prefect_test_harness():
        yield
