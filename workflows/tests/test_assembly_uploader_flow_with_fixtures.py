import json

import pytest
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models as mg_models
import ena.models as ena_models
from workflows.flows.assembly_uploader import (
    assembly_uploader
)


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_fixtures_usage(prefect_harness, ena_study_fixture):
    study_accession = "PRJ1"
    run_accession = "ERR1"

    await assembly_uploader(study_accession=study_accession, run_accession=run_accession, dry_run=True)