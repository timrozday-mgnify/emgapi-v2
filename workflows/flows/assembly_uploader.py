from datetime import timedelta
from enum import Enum
from typing import List

import django
from django.conf import settings
from prefect.input import RunInput
from prefect.task_runners import SequentialTaskRunner

from workflows.prefect_utils.slurm_flow import (
    run_cluster_jobs,
    slurm_status_is_finished_successfully,
    FINAL_SLURM_STATE,
)

django.setup()

import httpx

import ena.models
import analyses.models
from prefect import flow, task, suspend_flow_run


@flow(
    name="Sanity check and upload an assembly",
    log_prints=True,
    flow_run_name="Sanity check and upload: {assembly_id}",
    task_runner=SequentialTaskRunner,
)
def assembly_uploader(assembler_id: int):
    """ """
    assembly = analyses.models.Assembly.objects.get(id=assembler_id)
    print(f"my assembly is {assembly}")
