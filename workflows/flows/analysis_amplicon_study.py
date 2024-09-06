import csv
from datetime import timedelta
from pathlib import Path
from typing import Any, List, Union

import django

django.setup()
from django.conf import settings
from django.utils.text import slugify
from prefect import flow, task
from prefect.artifacts import create_table_artifact
from prefect.task_runners import SequentialTaskRunner

import analyses.models
from emgapiv2.settings import EMG_CONFIG
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_readruns_from_ena,
)
from workflows.nextflow_utils.samplesheets import (
    SamplesheetColumnSource,
    queryset_hash,
    queryset_to_samplesheet,
)
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)


@task(
    log_prints=True,
)
def get_runs_to_attempt(study: analyses.models.Study) -> List[Union[str, int]]:
    """
    Determine the list of runs worth trying currently for this study.
    :param study:
    :return:
    """
    study.refresh_from_db()
    runs_worth_trying = (
        study.analyses.filter(
            **{
                f"status__{analyses.models.Analysis.AnalysisStates.ANALYSIS_COMPLETED}": False,
                f"status__{analyses.models.Analysis.AnalysisStates.ANALYSIS_BLOCKED}": False,
            }
        )
        .filter(run__experiment_type=analyses.models.Run.ExperimentTypes.AMPLICON.value)
        .values_list("id", flat=True)
    )
    print(f"Got {len(runs_worth_trying)} run to attempt")
    return runs_worth_trying


@task(
    log_prints=True,
)
def create_analyses(study: analyses.models.Study, runs: List[str]):
    analyses_list = []
    for run in runs:
        run_obj = analyses.models.Run.objects.get(ena_accessions__contains=run)
        analysis, created = analyses.models.Analysis.objects.get_or_create(
            study=study, sample=run_obj.sample, run=run_obj, ena_study=study.ena_study
        )
        if created:
            print(f"Created analyses {analysis} {analysis.run.experiment_type}")
        analyses_list.append(analysis)
    return analyses_list


@task(log_prints=True)
def chunk_amplicon_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    return [items[j : j + chunk_size] for j in range(0, len(items), chunk_size)]


@task(log_prints=True)
def mark_analysis_as_started(analysis: analyses.models.Analysis):
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_STARTED)


@task(log_prints=True)
def mark_analysis_as_failed(analysis: analyses.models.Analysis):
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_FAILED)


@task(log_prints=True)
def mark_analysis_as_completed(analysis: analyses.models.Analysis):
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def make_samplesheet_amplicon(
    mgnify_study: analyses.models.Study,
    runs_ids: List[Union[str, int]],
) -> Path:
    runs = analyses.models.Run.objects.filter(id__in=runs_ids)

    ss_hash = queryset_hash(runs, "id")

    sample_sheet_tsv = queryset_to_samplesheet(
        queryset=runs,
        filename=Path(EMG_CONFIG.slurm.default_workdir)
        / Path(
            f"{mgnify_study.ena_study.accession}_samplesheet_amplicon-v6_{ss_hash}.csv"
        ),
        column_map={
            "sample": SamplesheetColumnSource(
                lookup_string="ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "fastq_1": SamplesheetColumnSource(
                lookup_string="metadata__fastq_ftps", renderer=lambda ftps: ftps[0]
            ),
            "fastq_2": SamplesheetColumnSource(
                lookup_string="metadata__fastq_ftps",
                renderer=lambda ftps: ftps[1] if len(ftps) > 1 else "",
            ),
            "single_end": SamplesheetColumnSource(
                lookup_string="metadata__fastq_ftps",
                renderer=lambda ftps: "false" if len(ftps) > 1 else "true",
            ),
        },
        bludgeon=True,
    )

    with open(sample_sheet_tsv) as f:
        csv_reader = csv.DictReader(f, delimiter="\t")
        table = list(csv_reader)

    create_table_artifact(
        key="amplicon-v6-initial-sample-sheet",
        table=table,
        description="Sample sheet created for run of amplicon-v6",
    )
    return sample_sheet_tsv


@flow(name="Run analysis pipeline-v6 in parallel", log_prints=True)
async def perform_amplicons_in_parallel(
    mgnify_study: analyses.models.Study,
    amplicon_ids: List[Union[str, int]],
    nextflow_profile: str,
):
    amplicon_analyses = analyses.models.Analysis.objects.select_related("run").filter(
        id__in=amplicon_ids
    )
    samplesheet = make_samplesheet_amplicon(mgnify_study, amplicon_ids)

    async for run in amplicon_analyses:
        mark_analysis_as_started(run)

    command = (
        f"nextflow run ebi-metagenomics/miassembler "
        f"-profile {nextflow_profile} "
        f"-resume "
        f"--input {samplesheet} "
        f"--outdir {mgnify_study.ena_study.accession}_amplicon_v6 "
        f"{'-with-tower' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        f"-name ampliocn-v6-for-samplesheet-{slugify(samplesheet)[-30:]} "
    )

    try:
        await run_cluster_job(
            name=f"Analyse amplicon study {mgnify_study.ena_study.accession} via samplesheet {slugify(samplesheet)}",
            command=command,
            expected_time=timedelta(days=5),
            memory=f"{EMG_CONFIG.slurm.amplicon_nextflow_master_job_memory}G",
            environment="ALL,TOWER_ACCESS_TOKEN,TOWER_WORKSPACE_ID",
        )
    except ClusterJobFailedException:
        for analysis in amplicon_analyses:
            mark_analysis_as_failed(analysis)
    else:
        # assume that if job finished, all assemblies finished...
        # todo: integrate per-run error handling
        for analysis in amplicon_analyses:
            mark_analysis_as_completed(analysis)

    # TODO: sanity check of results


@flow(
    name="Run analysis pipeline-v6 on amplicon study",
    log_prints=True,
    flow_run_name="Analyse amplicon: {study_accession}",
    task_runner=SequentialTaskRunner,
)
async def analysis_amplicon_study(
    study_accession: str, nextflow_profile: str = "codon_slurm"
):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off amplicon-v6 pipeline.
    :param study_accession: Study accession e.g. PRJxxxxxx
    :param profile: Name of the nextflow profile to use for amplicon-v6.
    """
    # Create/get ENA Study object
    ena_study = get_study_from_ena(study_accession)
    await ena_study.arefresh_from_db()
    print(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # Get a MGnify Study object for this ENA Study
    mgnify_study = await analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    await mgnify_study.arefresh_from_db()
    print(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    read_runs = get_study_readruns_from_ena(
        ena_study.accession, limit=5000, filter_library_strategy="AMPLICON"
    )
    print(f"Returned {len(read_runs)} run from ENA portal API")

    # get or create Analysis for runs
    mgnify_analyses = create_analyses(mgnify_study, read_runs)
    runs_to_attempt = get_runs_to_attempt(mgnify_study)

    # Work on chunks of 20 readruns at a time
    # Doing so means we don't use our entire cluster allocation for this study
    chunk_size = 20
    chunked_runs = chunk_amplicon_list(runs_to_attempt, chunk_size)
    print("chunked", chunked_runs)
    for runs_chunk in chunked_runs:
        # launch jobs for all runs in this chunk in a single flow
        print(f"Working on amplicons: {runs_chunk[0]}-{runs_chunk[len(runs_chunk)-1]}")
        await perform_amplicons_in_parallel(mgnify_study, runs_chunk, nextflow_profile)
