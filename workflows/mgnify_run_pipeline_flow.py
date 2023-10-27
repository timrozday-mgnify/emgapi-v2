import random
import time

import django
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from prefect_shell import ShellOperation

django.setup()


import analyses.models
from prefect import flow, task


@task(retries=1)
def start_analysis_pipeline(study_accession: str):
    fake_job_id = int(time.time() + 300)

    ShellOperation(
        commands=[
            "touch ${job_id}",  # e.g. bsub ....
        ],
        env={"job_id": fake_job_id}
    ).run()
    create_table_artifact(
        key="analysis-pipeline-jobs",
        table=[{
            "job_id": fake_job_id,
            "study": study_accession
        }]
    )
    return fake_job_id


@task(retries=50, retry_delay_seconds=10)
def await_analysis_pipeline(job_id: int):
    now = time.time()
    assert now > job_id


@task(retries=2)
def upload_analysis(study_accession: str):
    study_num = random.randint(0, 1000)
    analysis_num = random.randint(0, 1000)
    study = analyses.models.Study.objects.create(accession=f"MGYS{study_num:05}", ena_study_id=study_accession)
    analysis = analyses.models.Analysis.objects.create(study=study, accession=f"MGYA{analysis_num:05}")
    create_markdown_artifact(
        key="analysis-pipeline-results",
        markdown=f"""
        # Analysis {analysis.accession}
        Was run on study {study_accession}.
        Job finished :) 
        """
    )
    return analysis


@flow(name="Run Analysis Pipeline", log_prints=True)
def mgnify_run_pipeline_flow(accession: str):
    """
    Run the MGnify analysis pipeline – wait until done and upload result.
    :param accession: Study accesion eg. PRJxxxxxx
    """
    job_id = start_analysis_pipeline(accession)
    await_analysis_pipeline(job_id)
    upload_analysis(accession)
