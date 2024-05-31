from datetime import timedelta
from enum import Enum
from typing import List

import django
from django.conf import settings
from prefect.input import RunInput
from prefect.task_runners import SequentialTaskRunner

from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
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


@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Get study from ENA: {accession}",
    log_prints=True,
)
def get_study_from_ena(accession: str) -> ena.models.Study:
    if ena.models.Study.objects.filter(accession=accession).exists():
        return ena.models.Study.objects.get(accession=accession)
    print(f"Will fetch from ENA Portal API Study {accession}")
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=study_accession%3D{accession}%20OR%20secondary_study_accession%3D{accession}&limit=10&format=json&fields=study_title"
    )
    if portal.status_code == httpx.codes.OK:
        study, created = ena.models.Study.objects.get_or_create(
            accession=accession,
            defaults={
                "title": portal.json()[0]["study_title"]
                # TODO: more metadata
            },
        )
        return study
    else:
        print(f"Bad status! {portal.status_code} {portal}")


@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    cache_expiration=timedelta(minutes=10),
    task_run_name="Set up MGnify Study: {ena_accession}",
    log_prints=True,
)
def get_mgnify_study(ena_accession: str) -> analyses.models.Study:
    print(f"Will get/create MGnify study for {ena_accession}")
    ena_study = ena.models.Study.objects.get(accession=ena_accession)
    study, _ = analyses.models.Study.objects.get_or_create(
        ena_study=ena_study, title=ena_study.title
    )
    return study


@task(log_prints=True)
def mark_assembly_as_completed(assembly: analyses.models.Assembly):
    print(f"Assembly {assembly} (run {assembly.run}) is now assembled")
    assembly.mark_status(assembly.AssemblyStates.ASSEMBLY_COMPLETED)


@task(log_prints=True)
def mark_assembly_as_started(assembly: analyses.models.Assembly):
    assembly.mark_status(assembly.AssemblyStates.ASSEMBLY_STARTED)


class AssemblerChoices(str, Enum):
    pipeline_default = "pipeline_default"
    megahit = "megahit"
    metaspades = "metaspades"
    spades = "spades"


class AssemblerInput(RunInput):
    assembler: AssemblerChoices
    memory_gb: int


@task(
    retries=10,
    retry_delay_seconds=60,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Get study readruns from ENA: {accession}",
    log_prints=True,
)
def get_study_readruns_from_ena(accession: str, limit: int = 20) -> List[str]:
    print(f"Will fetch study {accession} read-runs from ENA portal API")
    mgys_study = analyses.models.Study.objects.get(ena_study__accession=accession)
    portal = httpx.get(
        f"https://www.ebi.ac.uk/ena/portal/api/links/study?accession={accession}&result=read_run&limit={limit}&format=json"
    )
    if portal.status_code == httpx.codes.OK:
        for read_run in portal.json():
            analyses.models.Run.objects.get_or_create(
                ena_accessions=[read_run["run_accession"]],
                study=mgys_study,
                ena_study=mgys_study.ena_study,
            )

    mgys_study.refresh_from_db()
    return [run.ena_accessions[0] for run in mgys_study.runs.all()]


@task(
    retries=2,
    task_run_name="Create/get assembly objects for read_runs in study: {study.accession}",
    log_prints=True,
)
def get_or_create_assemblies_for_runs(
    study: ena.models.Study, read_runs: List[str]
) -> List[str]:
    assembly_ids = []
    for read_run in read_runs:
        run = analyses.models.Run.objects.get(ena_accessions__contains=read_run)
        assembly, created = analyses.models.Assembly.objects.get_or_create(
            run=run, ena_study=study
        )
        if created:
            print(f"Created assembly {assembly}")
        assembly_ids.append(assembly.id)
    return assembly_ids


@flow(
    name="Assemble a study",
    log_prints=True,
    flow_run_name="Assemble: {accession}",
    task_runner=SequentialTaskRunner,
)
async def assemble_study(accession: str):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off assembly pipeline.
    :param accession: Study accession e.g. PRJxxxxxx
    """

    # Create (or get) an ENA Study object, populating with metadata from ENA
    # Refresh from DB in case we get an old cached version.
    ena_study = get_study_from_ena(accession).refresh_from_db()

    print(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # Get a MGnify Study object for this ENA Study
    mgnify_study = get_mgnify_study(accession).refresh_from_db()
    print(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    read_runs = get_study_readruns_from_ena(ena_study.accession, limit=5000)
    print(f"Have {len(read_runs)} from ENA portal API")

    assembler_input: AssemblerInput = await suspend_flow_run(
        wait_for_input=AssemblerInput.with_initial_data(
            assembler=AssemblerChoices.pipeline_default,
            memory_gb=8,
            description=f"""
**MI-Assembler**
This will assemble all {len(read_runs)} read-runs of study {ena_study.accession}
using [MI-Assembler](https://www.github.com/ebi-metagenomics/mi-assembler).

Please pick which assembler tool to use, or let the pipeline choose for you.
Also select how much RAM (in GB) to allocate for each assembly.
            """,
        )
    )
    print(f"Using assembler: {assembler_input}")

    assembler_command_arg = (
        f"--assembler {assembler_input.assembler.value}"
        if assembler_input.assembler != AssemblerChoices.pipeline_default
        else ""
    )

    assemblies = get_or_create_assemblies_for_runs(ena_study, read_runs)

    # Work on chunks of 20 readruns at a time
    # Doing so means we don't use our entire cluster allocation for this study
    chunk_size = 20
    chunked_assemblies = [
        assemblies[j : j + chunk_size] for j in range(0, len(assemblies), chunk_size)
    ]
    for assembly_chunk in chunked_assemblies:
        # launch jobs for all runs in this chunk in a single flow
        for assembly_id in assembly_chunk:
            assembly = await analyses.models.Assembly.objects.aget(id=assembly_id)
            mark_assembly_as_started(assembly)

        read_runs_chunk = [
            await analyses.models.Run.objects.aget(assemblies__id=assem)
            for assem in assembly_chunk
        ]

        chunk_jobs = await run_cluster_jobs(
            names=[
                f"Assemble read_run {run.first_accession} for study {ena_study.accession}"
                for run in read_runs_chunk
            ],
            commands=[
                f"nextflow run {settings.EMG_CONFIG.slurm.pipelines_root_dir}/miassembler/main.nf "
                f"-profile codon_slurm "
                f"-resume "
                f"--outdir {ena_study.accession}_{run.first_accession}_miassembler "
                f"--study_accession {ena_study.accession} "
                f"--reads_accession {run.first_accession} "
                f"{assembler_command_arg} "
                f"{'-with-tower' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
                f"-name mi-assembler-{run.first_accession}-for-{ena_study.accession} "
                for run in read_runs_chunk
            ],
            expected_time=timedelta(days=5),
            memory=f"{assembler_input.memory_gb}G",
            environment="ALL,TOWER_ACCESS_TOKEN,TOWER_WORKSPACE_ID",  # will copy this env from the prefect worker to the jobs
            raise_on_job_failure=False,
        )

        for assembly_id, assembly_job in zip(assembly_chunk, chunk_jobs):
            if slurm_status_is_finished_successfully(assembly_job[FINAL_SLURM_STATE]):
                assembly = await analyses.models.Assembly.objects.aget(id=assembly_id)
                mark_assembly_as_completed(assembly)
            else:
                assembly = await analyses.models.Assembly.objects.aget(id=assembly_id)
                assembly.mark_status(assembly.AssemblyStates.ASSEMBLY_FAILED)
