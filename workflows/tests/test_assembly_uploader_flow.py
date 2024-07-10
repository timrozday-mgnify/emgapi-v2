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
async def test_prefect_assembly_upload_flow(prefect_harness):
    ### ENA MOCKING ###
    study_accession = "PRJ1"
    run_accession = "ERR1"
    sample_accession = "SAMP1"

    # create DB records for tests
    ena_study = await ena_models.Study.objects.acreate(accession=study_accession, title="Project 1")
    ena_sample = await ena_models.Sample.objects.acreate(study=ena_study,
                                                        metadata={"accession": sample_accession,
                                                                  "description": "Sample 1"})

    mgnify_study = await mg_models.Study.objects.acreate(ena_study=ena_study, title="Project 1", )
    mgnify_sample = await mg_models.Sample.objects.acreate(ena_sample=ena_sample, ena_study=ena_sample.study)
    mgnify_run = await mg_models.Run.objects.acreate(ena_accessions=[run_accession], study=mgnify_study,
                                              ena_study=mgnify_sample.ena_study, sample=mgnify_sample)
    mgnify_assembly = await mg_models.Assembly.objects.acreate(run=mgnify_run, reads_study=mgnify_study,
                                                        ena_study=mgnify_run.ena_study,
                                                        dir='slurm/fs/hps/tests/assembly_uploader')

    await assembly_uploader(study_accession=study_accession, run_accession=run_accession, dry_run=True)