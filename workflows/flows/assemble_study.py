import csv
from collections import defaultdict
from datetime import timedelta
from enum import Enum
from pathlib import Path
from textwrap import dedent as _
from typing import Any, List, Union

import django
import pandas as pd
from asgiref.sync import sync_to_async
from django.conf import settings
from django.utils.text import slugify
from prefect.artifacts import create_table_artifact
from prefect.input import RunInput
from prefect.task_runners import SequentialTaskRunner

from workflows.ena_utils.ena_file_fetching import convert_ena_ftp_to_fire_fastq
from workflows.views import encode_samplesheet_path

django.setup()

import httpx
from prefect import flow, suspend_flow_run, task

import analyses.models
import ena.models
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
from workflows.prefect_utils.analyses_models_helpers import task_mark_assembly_status
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)


@task()
def get_memory_for_assembler(
    biome: analyses.models.Biome,
    assembler: analyses.models.Assembler,
):
    assembler_heuristics = analyses.models.ComputeResourceHeuristic.objects.filter(
        process=analyses.models.ComputeResourceHeuristic.ProcessTypes.ASSEMBLY,
        assembler=assembler,
    )

    # ascend the biome hierarchy to find a memory heuristic
    for biome_to_try in biome.ancestors().reverse():
        heuristic = assembler_heuristics.filter(biome=biome_to_try).first()
        if heuristic:
            return heuristic.memory_gb


class AssemblerChoices(str, Enum):
    # IDEA: it would be nice to sniff this from the pipeline schema
    pipeline_default = "pipeline_default"
    megahit = "megahit"
    metaspades = "metaspades"
    spades = "spades"


class AssemblerInput(RunInput):
    assembler: AssemblerChoices
    memory_gb: int


def get_biomes_as_choices():
    # IDEA: move this one to a helper of some sorts
    biomes = {
        str(biome.path): biome.pretty_lineage
        for biome in analyses.models.Biome.objects.all()
    }
    BiomeChoices = Enum("BiomeChoices", biomes)
    return BiomeChoices


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
        f'https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&dataPortal=metagenome&format=json&fields=sample_accession,sample_title,secondary_sample_accession,fastq_md5,fastq_ftp,library_layout,library_strategy&query="study_accession={accession} OR secondary_study_accession={accession}"&limit={limit}&format=json'
    )

    if portal.status_code == httpx.codes.OK:
        for read_run in portal.json():
            ena_sample, _ = ena.models.Sample.objects.get_or_create(
                accession=read_run["sample_accession"],
                defaults={
                    "metadata": {"sample_title": read_run["sample_title"]},
                    "study": mgys_study.ena_study,
                },
            )

            mgnify_sample, _ = analyses.models.Sample.objects.update_or_create(
                ena_sample=ena_sample,
                defaults={
                    "ena_accessions": [
                        read_run["sample_accession"],
                        read_run["secondary_sample_accession"],
                    ],
                    "ena_study": mgys_study.ena_study,
                },
            )

            run, _ = analyses.models.Run.objects.update_or_create(
                ena_accessions=[read_run["run_accession"]],
                study=mgys_study,
                ena_study=mgys_study.ena_study,
                sample=mgnify_sample,
                defaults={
                    "metadata": {
                        "library_strategy": read_run["library_strategy"],
                        "library_layout": read_run["library_layout"],
                        "fastq_ftps": list(read_run["fastq_ftp"].split(";")),
                    },
                },
            )
            run.set_experiment_type_by_ena_library_strategy(
                read_run["library_strategy"]
            )

    mgys_study.refresh_from_db()
    return [run.ena_accessions[0] for run in mgys_study.runs.all()]


@task(
    retries=2,
    task_run_name="Create/get assembly objects for read_runs in study: {study.accession}",
    log_prints=True,
)
def get_or_create_assemblies_for_runs(
    study: analyses.models.Study, read_runs: List[str]
) -> List[str]:
    assembly_ids = []
    for read_run in read_runs:
        run = analyses.models.Run.objects.get(ena_accessions__icontains=read_run)
        if run.experiment_type not in [run.ExperimentTypes.METAGENOMIC]:
            print(
                f"Not creating assembly for run {run.first_accession} because it is a {run.experiment_type}"
            )
            continue

        assembly, created = analyses.models.Assembly.objects.get_or_create(
            run=run, ena_study=study.ena_study, reads_study=study
        )
        if created:
            print(f"Created assembly {assembly}")
        assembly_ids.append(assembly.id)
    return assembly_ids


@task(
    log_prints=True,
)
def get_assemblies_to_attempt(study: analyses.models.Study) -> List[Union[str, int]]:
    """
    Determine the list of assemblies worth trying currently for this study.
    :param study:
    :return:
    """
    study.refresh_from_db()
    assemblies_worth_trying = study.assemblies_reads.filter(
        **{
            f"status__{analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED}": False,
            f"status__{analyses.models.Assembly.AssemblyStates.ASSEMBLY_BLOCKED}": False,
        }
    ).values_list("id", flat=True)
    return assemblies_worth_trying


@task
def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    return [items[j : j + chunk_size] for j in range(0, len(items), chunk_size)]


EXPERIMENT_TYPES_TO_MIASSEMBLER_LIBRARY_STRATEGY = defaultdict(
    lambda: "other",
    **{
        analyses.models.Run.ExperimentTypes.METAGENOMIC: "metagenomic",
        analyses.models.Run.ExperimentTypes.METATRANSCRIPTOMIC: "metatranscriptomic",
    },
)


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def make_samplesheet(
    mgnify_study: analyses.models.Study,
    assembly_ids: List[Union[str, int]],
    assembler: analyses.models.Assembler,
) -> Path:
    assemblies = analyses.models.Assembly.objects.select_related("run").filter(
        id__in=assembly_ids
    )

    ss_hash = queryset_hash(assemblies, "id")

    memory = get_memory_for_assembler(mgnify_study.biome, assembler)

    sample_sheet_tsv = queryset_to_samplesheet(
        queryset=assemblies,
        filename=Path(EMG_CONFIG.slurm.default_workdir)
        / Path(
            f"{mgnify_study.ena_study.accession}_samplesheet_miassembler_{ss_hash}.csv"
        ),
        column_map={
            "study_accession": SamplesheetColumnSource(
                lookup_string="ena_study__accession"
            ),
            "reads_accession": SamplesheetColumnSource(
                lookup_string="run__ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "library_strategy": SamplesheetColumnSource(
                lookup_string="run__experiment_type",
                renderer=EXPERIMENT_TYPES_TO_MIASSEMBLER_LIBRARY_STRATEGY.get,
            ),
            "library_layout": SamplesheetColumnSource(
                lookup_string="run__metadata__library_layout",
                renderer=lambda layout: str(layout).lower(),
            ),
            "fastq_1": SamplesheetColumnSource(
                lookup_string="run__metadata__fastq_ftps",
                renderer=lambda ftps: convert_ena_ftp_to_fire_fastq(ftps[0]),
            ),
            "fastq_2": SamplesheetColumnSource(
                lookup_string="run__metadata__fastq_ftps",
                renderer=lambda ftps: (
                    convert_ena_ftp_to_fire_fastq(ftps[1]) if len(ftps) > 1 else ""
                ),
            ),
            "assembler": SamplesheetColumnSource(
                lookup_string="id", renderer=lambda _: assembler.name.lower()
            ),
            "assembly_memory": SamplesheetColumnSource(
                lookup_string="id", renderer=lambda _: memory
            ),
        },
        bludgeon=True,
    )

    with open(sample_sheet_tsv) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        table = list(csv_reader)

    create_table_artifact(
        key="miassembler-initial-sample-sheet",
        table=table,
        description=_(
            f"""\
            Sample sheet created for run of MIAssembler.
            Saved to `{sample_sheet_tsv}`
            [Edit it]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(sample_sheet_tsv)})
            """
        ),
    )
    return sample_sheet_tsv


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def make_samplesheets_for_runs_to_assemble(
    mgnify_study: analyses.models.Study,
    assembler: analyses.models.Assembler,
    chunk_size: int = 10,
) -> [Path]:
    assemblies_to_attempt = get_assemblies_to_attempt(mgnify_study)
    chunked_assemblies = chunk_list(assemblies_to_attempt, chunk_size)

    sheets = [
        make_samplesheet(mgnify_study, assembly_chunk, assembler)
        for assembly_chunk in chunked_assemblies
    ]
    return sheets


###################
# Assembler flow #
##################


@flow(flow_run_name="Assemble {samplesheet_csv}")
async def run_assembler_for_samplesheet(
    mgnify_study: analyses.models.Study,
    samplesheet_csv: Path,
    miassembler_profile: str,
    assembler: analyses.models.Assembler,
    memory_gb: int,
):
    samplesheet_df = pd.read_csv(samplesheet_csv, sep=",")
    assemblies: list[analyses.models.Assembly] = mgnify_study.assemblies_reads.filter(
        run__ena_accessions__0__in=samplesheet_df["reads_accession"]
    )

    async for assembly in assemblies:
        task_mark_assembly_status(
            assembly, status=assembly.AssemblyStates.ASSEMBLY_STARTED
        )

    command = (
        f"nextflow run ebi-metagenomics/miassembler "
        f"-r main "  # From the main branch (which is the stable one)
        f"-profile {miassembler_profile} "
        f"-resume "
        f"--samplesheet {samplesheet_csv} "
        f"--outdir {EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_miassembler "
        f"--assembler {assembler.name.lower()} "
        f"{'-with-tower' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        f"-name mi-assembler-for-samplesheet-{slugify(samplesheet_csv)[-30:]} "
    )

    try:
        await run_cluster_job(
            name=f"Assemble study {mgnify_study.ena_study.accession} via samplesheet {slugify(samplesheet_csv)}",
            command=command,
            expected_time=timedelta(days=5),
            memory=f"{memory_gb}G",
            environment="ALL,TOWER_ACCESS_TOKEN,TOWER_WORKSPACE_ID",
        )
    except ClusterJobFailedException:
        for assembly in assemblies:
            task_mark_assembly_status(
                assembly, status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED
            )
    else:
        # The pipeline produces top level end of execution reports, which contain
        # the list of the runs that were assembled, and those that were not.
        # For more information: https://github.com/EBI-Metagenomics/miassembler?tab=readme-ov-file#top-level-reports

        # QC failed / not assembled runs: qc_failed_runs.csv
        # Assembled runs: assembled_runs.csv

        qc_failed_csv = Path(
            f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_miassembler/qc_failed_runs.csv"
        )
        qc_failed_runs = {}  # Stores {run_accession, qc_fail_reason}

        if qc_failed_csv.is_file():
            with qc_failed_csv.open(mode="r") as file_handle:
                for row in csv.reader(file_handle, delimiter=","):
                    run_accession, fail_reason = row
                    qc_failed_runs[run_accession] = fail_reason

        assembled_runs_csv = Path(
            f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_miassembler/assembled_runs.csv"
        )
        assembled_runs = set()

        if not assembled_runs_csv.is_file():
            for assembly in assemblies:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    reason=f"The miassembler output is missing the {assembled_runs_csv} file.",
                )
            raise Exception(
                f"Missing end of execution assembled runs csv file. Expected path {assembled_runs_csv}."
            )

        with assembled_runs_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                run_accession, assembler_software, assembler_version = row
                assembled_runs.add(run_accession)

        for assembly in assemblies:
            if assembly.run.first_accession in qc_failed_runs:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
                    reason=qc_failed_runs[assembly.run.first_accession],
                )
            elif assembly.run.first_accession in assembled_runs:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                )
            else:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    reason="The assembly is missing from the pipeline end-of-run reports",
                )


@flow(
    name="Assemble a study",
    log_prints=True,
    flow_run_name="Assemble: {accession}",
    task_runner=SequentialTaskRunner,
)
async def assemble_study(accession: str, miassembler_profile: str = "codon_slurm"):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off assembly pipeline.
    :param accession: Study accession e.g. PRJxxxxxx
    :param miassembler_profile: Name of the nextflow profile to use for MI Assembler.
    """

    # Create (or get) an ENA Study object, populating with metadata from ENA
    # Refresh from DB in case we get an old cached version.
    ena_study = get_study_from_ena(accession)
    await ena_study.arefresh_from_db()
    print(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # define this within flow because it dynamically creates options from DB.
    BiomeChoices = await sync_to_async(get_biomes_as_choices)()

    class BiomeInput(RunInput):
        biome: BiomeChoices

    biome_input: BiomeInput = await suspend_flow_run(
        wait_for_input=BiomeInput.with_initial_data(
            description=f"""
**Biome tagger**
Please select a Biome for the entire study [{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession}).

The Biome is important metadata, and will also be used to guess how much memory is needed to assemble this study.
        """
        )
    )

    biome = await analyses.models.Biome.objects.aget(path=biome_input.biome.name)

    # Get a MGnify Study object for this ENA Study
    mgnify_study = await analyses.models.Study.objects.get_or_create_for_ena_study(
        accession
    )
    mgnify_study.biome = biome
    await mgnify_study.asave()
    print(
        f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}. Biome: {biome.path}"
    )

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
Also optionally select how much RAM (in GB) to allocate for each nextflow head job.
(The RAM for the assembly itself is determined by the `ComputeResourceHeuristics`.)
            """,
        )
    )
    print(f"Using assembler name: {assembler_input}")
    assembler_name = assembler_input.assembler
    if assembler_name == AssemblerChoices.pipeline_default:
        assembler_name = analyses.models.Assembler.assembler_default

    assembler = (
        await analyses.models.Assembler.objects.filter(name__iexact=assembler_name)
        .order_by("-version")
        .afirst()
    )
    # assumes latest version...

    get_or_create_assemblies_for_runs(mgnify_study, read_runs)
    samplesheets = make_samplesheets_for_runs_to_assemble(mgnify_study, assembler)
    for samplesheet in samplesheets:
        await run_assembler_for_samplesheet(
            mgnify_study,
            samplesheet,
            miassembler_profile,
            assembler,
            assembler_input.memory_gb,
        )
