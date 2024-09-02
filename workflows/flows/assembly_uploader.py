import gzip
import os
import re
from datetime import timedelta

import django
from Bio import SeqIO

django.setup()

from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from prefect import flow, get_run_logger, task
from prefect.task_runners import SequentialTaskRunner

import analyses.models
from emgapiv2.settings import EMG_CONFIG
from workflows.prefect_utils.analyses_models_helpers import task_mark_assembly_status
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.shell_task import run_shell_command
from workflows.prefect_utils.slurm_flow import (
    FINAL_SLURM_STATE,
    SLURM_JOB_ID,
    run_cluster_jobs,
    slurm_status_is_finished_successfully,
)

OPTIONAL_SPADES_FILES = [
    ".assembly_graph.fastg.gz",
    ".assembly_graph_with_scaffolds.gfa.gz",
    ".scaffolds.fa.gz",
]
SPADES_PARAMS = "params.txt"


@task(
    retries=2,
    task_run_name="Sanity check: {accession}",
)
def check_assembly(accession, assembly_path, assembler):
    logger = get_run_logger()
    logger.info(f"Check assembly {accession}")
    # check filesize
    if not os.path.exists(assembly_path):
        logger.warning(f"{assembly_path} does not exist")
        return False
    file_size = os.path.getsize(assembly_path)
    if not file_size:
        logger.warning(f"Contigs file is empty for assembly {accession}")
        return False
    # check spades files
    if "spades" in assembler:
        for postfix in OPTIONAL_SPADES_FILES:
            if not os.path.exists(
                os.path.join(os.path.dirname(assembly_path), accession + postfix)
            ):
                logger.warning(f"{accession + postfix} does not exist")
        if not os.path.exists(
            os.path.join(os.path.dirname(assembly_path), SPADES_PARAMS)
        ):
            logger.warning(f"{SPADES_PARAMS} does not exist")

    # check number of contigs
    # TODO if we gonna have assembly_job.result.num_contigs record in DB - reuse it
    count = 0
    with gzip.open(assembly_path, "rt") as handle:
        for _ in SeqIO.parse(handle, "fasta"):
            count += 1
            if count >= 2:
                return True
    if count < 2:
        logger.warning(
            f"Number of contigs in assembly file {assembly_path} is less than 2"
        )
        return False
    return True


@task
def check_study_registration_existence(study, run_accession):
    completed_assemblies = (
        study.assemblies_reads.filter(
            **{
                f"status__{analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED}": True,
            }
        )
        .exclude(run__ena_accessions__in=[run_accession])
        .values_list("id", flat=True)
    )
    if len(completed_assemblies) > 0:
        return True
    else:
        return False


def define_library(run_experiment_type):
    logger = get_run_logger()
    library = ""
    if run_experiment_type == analyses.models.Run.ExperimentTypes.METAGENOMIC:
        library = "metagenome"
    elif run_experiment_type == analyses.models.Run.ExperimentTypes.METATRANSCRIPTOMIC:
        library = "metatranscriptome"
    else:
        logger.warning(f"Unsupported experiment type {run_experiment_type}")
    return library


@task(
    retries=2,
    task_run_name="Create study XML: {study_accession}",
)
async def create_study_xml(study_accession, library, output_dir):
    # if assembly_uploader_root_dir set in config - use script from assembly_uploader installation
    # otherwise means installation was done into env and can be launched with study_xmls command
    logger = get_run_logger()
    if EMG_CONFIG.slurm.assembly_uploader_root_dir:
        # take from root_dir installation
        launcher = (
            f"{EMG_CONFIG.slurm.assembly_uploader_python_executable} "
            f"{os.path.join(EMG_CONFIG.slurm.assembly_uploader_root_dir, 'assembly_uploader', 'study_xmls.py')}"
        )
    else:
        # take from env installation
        launcher = "study_xmls"
    command = (
        f"{launcher} "
        f"--study {study_accession} "
        f"--library {library} "
        f"--output-dir {output_dir} "
        f"--center EMG "
    )
    try:
        await run_shell_command(
            command,
            env={
                "ENA_WEBIN": EMG_CONFIG.webin.emg_webin_account,
                "ENA_WEBIN_PASSWORD": EMG_CONFIG.webin.emg_webin_password,
            },
            workdir=output_dir,
        )
    except Exception as e:
        logger.error(f"Command failed with error: {e}")
    # check was upload folder created or not
    upload_folder = os.path.join(output_dir, study_accession + "_upload")
    logger.info(f"Upload folder: {os.path.abspath(upload_folder)}")
    if os.path.exists(upload_folder):
        if len([i for i in os.listdir(upload_folder) if i.endswith(".xml")]) != 2:
            raise Exception("Folder {upload_folder} should contain 2 XMLs")
        else:
            logger.info(f"Upload folder {upload_folder} and study XMLs were created")
    else:
        raise Exception(f"Folder {upload_folder} does not exist")


@task(
    retries=2,
    task_run_name="Submit study XML: {study_accession}",
)
async def submit_study_xml(study_accession, upload_dir, dry_run):
    logger = get_run_logger()
    # if assembly_uploader_root_dir set in config - use script from assembly_uploader installation
    # otherwise means installation was done into env and can be launched with submit_study command
    if EMG_CONFIG.slurm.assembly_uploader_root_dir:
        # take from root_dir installation
        launcher = (
            f"{EMG_CONFIG.slurm.assembly_uploader_python_executable} "
            f"{os.path.join(EMG_CONFIG.slurm.assembly_uploader_root_dir, 'assembly_uploader', 'submit_study.py')}"
        )
    else:
        # take from env installation
        launcher = "submit_study"
    command = f"{launcher} " f"--study {study_accession} " f"--directory {upload_dir} "
    if dry_run:
        command += "--test "
    try:
        _, output_log = await run_shell_command(
            command,
            env={
                "ENA_WEBIN": EMG_CONFIG.webin.emg_webin_account,
                "ENA_WEBIN_PASSWORD": EMG_CONFIG.webin.emg_webin_password,
            },
        )
        # get a registered study accession from logs
        if (
            "Make a note of this!" in output_log
            or "An accession with this alias already exists in project" in output_log
        ):
            new_acc = re.findall(
                rf"{EMG_CONFIG.ena.primary_study_accession_re}", output_log
            )
            if new_acc:
                return new_acc[0]
            else:
                raise Exception(
                    f"No assigned assembly study accession was found for {study_accession}"
                )
        else:
            raise Exception(
                f"No assembly study accession was found in uploader log for {study_accession}"
            )

    except Exception as e:
        logger.error(f"Command failed with error: {e}")


@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Check study registration, create study XML and submit to ENA",
)
async def process_study(
    study_accession: str,
    mgnify_study: analyses.models.Study,
    run_accession: str,
    assembly_path: str,
    mgnify_run: analyses.models.Run,
    upload_folder: str,
    dry_run: bool,
):
    logger = get_run_logger()
    registered_study = None
    # check a first completed assembly and register study only once
    if not check_study_registration_existence(mgnify_study, run_accession):
        logger.info(f"Register study: {study_accession}")
        await create_study_xml(
            study_accession=study_accession,
            library=define_library(mgnify_run.experiment_type),
            output_dir=os.path.dirname(assembly_path),
        )

        logger.info(f"Submit study: {study_accession}")
        registered_study = await submit_study_xml(
            study_accession=study_accession, upload_dir=upload_folder, dry_run=dry_run
        )
        if registered_study:
            logger.info(f"Study submitted successfully under {registered_study}")
    return registered_study


@task(
    retries=2,
    task_run_name="Generate csv for upload: {run_accession}",
)
def generate_assembly_csv(
    metadata_dir, run_accession, coverage, assembler, assembler_version, assembly_path
):
    logger = get_run_logger()
    assembly_csv = os.path.join(metadata_dir, run_accession + ".csv")
    with open(assembly_csv, "w") as file_out:
        file_out.write(
            ",".join(["Run", "Coverage", "Assembler", "Version", "Filepath"]) + "\n"
        )
        line = ",".join(
            [
                run_accession,
                str(coverage),
                assembler,
                assembler_version,
                os.path.abspath(assembly_path),
            ]
        )
        file_out.write(line + "\n")
    logger.info(f"CSV: {os.path.abspath(assembly_csv)}")
    return assembly_csv


@task(
    retries=2,
    task_run_name="Generate assembly manifest: {run_accession}",
)
async def generate_assembly_xml(
    study_accession, run_accession, data_csv_path, registered_study, upload_dir
):
    logger = get_run_logger()
    # if assembly_uploader_root_dir set in config - use script from assembly_uploader installation
    # otherwise means installation was done into env and can be launched with assembly_manifest command
    if EMG_CONFIG.slurm.assembly_uploader_root_dir:
        # take from root_dir installation
        launcher = (
            f"{EMG_CONFIG.slurm.assembly_uploader_python_executable} "
            f"{os.path.join(EMG_CONFIG.slurm.assembly_uploader_root_dir, 'assembly_uploader', 'assembly_manifest.py')}"
        )
    else:
        # take from env installation
        launcher = "assembly_manifest"
    command = (
        f"{launcher} "
        f"--study {study_accession} "
        f"--data {data_csv_path} "
        f"--assembly_study {registered_study} "
        f"--output-dir {upload_dir} "
        f"--force "
    )
    try:
        await run_shell_command(
            command,
            env={
                "ENA_WEBIN": EMG_CONFIG.webin.emg_webin_account,
                "ENA_WEBIN_PASSWORD": EMG_CONFIG.webin.emg_webin_password,
            },
        )
        return True
    except Exception as e:
        logger.error(f"Command failed with error: {e}")
        return False


@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Create metadata and assembly manifest",
)
async def prepare_assembly(
    run_accession: str,
    study_accession: str,
    mgnify_assembly: analyses.models.Assembly,
    assembler: str,
    assembler_version: str,
    assembly_path: str,
    registered_study: str,
    upload_folder: str,
):
    logger = get_run_logger()
    upload_output_folder = os.path.dirname(assembly_path)
    # create metadata table
    metadata_dir = os.path.join(upload_folder, "metadata")
    if not os.path.exists(metadata_dir):
        os.mkdir(metadata_dir)
    logger.info(f"Generate csv for assembly {run_accession}")
    data_csv_path = generate_assembly_csv(
        metadata_dir=metadata_dir,
        run_accession=run_accession,
        coverage=mgnify_assembly.metadata["coverage"],
        assembler=assembler,
        assembler_version=assembler_version,
        assembly_path=assembly_path,
    )
    # assembly_uploader assembly_manifest
    logger.info("Generate assembly manifests")
    await generate_assembly_xml(
        study_accession=study_accession,
        run_accession=run_accession,
        data_csv_path=data_csv_path,
        registered_study=registered_study,
        upload_dir=upload_output_folder,
    )
    manifest = os.path.join(upload_folder, run_accession + ".manifest")
    return manifest


@task(
    retries=2,
    task_run_name="Get assembly accession for {run_accession}",
)
async def get_assigned_assembly_accession(run_accession, upload_dir):
    logger = get_run_logger()
    webin_cli_log = os.path.join(upload_dir, "webin-cli.report")
    if os.path.exists(webin_cli_log):
        with open(webin_cli_log, "r") as report:
            report_lines = report.readlines()
            assembly_accession_match_list = re.findall(
                EMG_CONFIG.ena.assembly_accession_re, ",".join(report_lines)
            )
            if assembly_accession_match_list:
                logger.info(f"Got {assembly_accession_match_list[0]}")
                return assembly_accession_match_list[0]
            else:
                logger.warning(f"No accession found in {webin_cli_log}")
                return None
    else:
        logger.warning(f"No {webin_cli_log} found")
        return None


@task()
def add_erz_accession(assembly: analyses.models.Assembly, erz_accession):
    assembly.add_erz_accession(erz_accession)


@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Run Webin-cli {run_accession}",
)
async def submit_assembly_slurm(
    run_accession: str,
    manifest: str,
    mgnify_assembly: analyses.models.Assembly,
    upload_folder: str,
    dry_run: bool,
):
    logger = get_run_logger()
    command = (
        f"java -Xms2G -jar {EMG_CONFIG.slurm.webin_cli_executor} "
        f"-context=genome "
        f"-manifest={os.path.abspath(manifest)} "
        f"-userName='{EMG_CONFIG.webin.emg_webin_account}' "
        f"-password='{EMG_CONFIG.webin.emg_webin_password}' "
    )
    if dry_run:
        command += "-test -validate "
    else:
        command += "-submit "

    slurm_job_results = await run_cluster_jobs(
        names=[f"Upload assembly for {run_accession} to ENA"],
        commands=[command],
        expected_time=timedelta(hours=1),
        memory=f"500M",
        environment="ALL",  # copy env vars from the prefect agent into the slurm job
        raise_on_job_failure=False,  # allows some jobs to fail without failing everything
    )

    if slurm_status_is_finished_successfully(slurm_job_results[0][FINAL_SLURM_STATE]):
        logger.info(f"Successfully ran webin-cli upload for {run_accession}")
        if dry_run:
            # no webin.report generated
            task_mark_assembly_status(
                mgnify_assembly, status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOADED
            )
        else:
            # check webin.report for ERZ
            erz_accession = await get_assigned_assembly_accession(
                run_accession, upload_folder
            )
            if erz_accession:
                logger.info(f"Upload completed for {run_accession} as {erz_accession}")
                add_erz_accession(mgnify_assembly, erz_accession)
                task_mark_assembly_status(
                    mgnify_assembly,
                    status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOADED,
                )
            else:
                logger.info(f"Upload failed for {run_accession}")
                task_mark_assembly_status(
                    mgnify_assembly,
                    status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
                )
    else:
        logger.error(
            f"Something went wrong running webin-cli upload for {run_accession} in job {slurm_job_results[0][SLURM_JOB_ID]}"
        )
        task_mark_assembly_status(
            mgnify_assembly,
            status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
        )


##########################
# Assembly uploader flow #
##########################


@flow(
    name="Sanity check and upload an assembly",
    flow_run_name=f"Sanity check and upload",
    task_runner=SequentialTaskRunner,
)
async def assembly_uploader(
    study_accession: str,
    run_accession: str,
    assembler: str = EMG_CONFIG.assembler.assembler_default,
    assembler_version: str = EMG_CONFIG.assembler.assembler_version_default,
    dry_run=True,
):
    """
    This flow performs a sanity check and uploads an assembly for a specific run to ENA.

    It is intended to be executed *per run* after the assembly flow. The assembly uploader
    scripts are executed using Prefect's `ShellOperation` command. The assembly submission
    via `webin-cli` is launched as a SLURM cluster job.
    """
    logger = get_run_logger()
    mgnify_study = await analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    mgnify_run = await analyses.models.Run.objects.aget(
        ena_accessions__icontains=run_accession
    )
    logger.info(f"MGnify data returned: study {mgnify_study.accession}, {mgnify_run}")

    try:
        mgnify_assembly: analyses.models.Assembly = (
            await analyses.models.Assembly.objects.filter(
                run=mgnify_run,
                reads_study=mgnify_study,
                assembler__name=assembler,
                assembler__version=assembler_version,
            ).aget()
        )
    except (MultipleObjectsReturned, ObjectDoesNotExist) as e:
        logger.error(
            f"Problem getting assembly for {mgnify_run} assembled with {assembler}_v{assembler_version} from ENA models DB"
        )
        raise e

    assembly_path = os.path.join(mgnify_assembly.dir, f"{run_accession}.fasta.gz")
    upload_folder = os.path.join(
        os.path.dirname(assembly_path), study_accession + "_upload"
    )
    logger.info(f"Assembly ID:{mgnify_assembly.id} found in {mgnify_assembly.dir}")

    # Sanity check
    logger.info(f"Processing assembly for: {run_accession}")
    if check_assembly(run_accession, assembly_path, assembler):
        logger.info(f"Assembly for {run_accession} passed sanity check")
    else:
        task_mark_assembly_status(
            mgnify_assembly,
            status=mgnify_assembly.AssemblyStates.POST_ASSEMBLY_QC_FAILED,
        )
        raise Exception(
            f"Assembly for {run_accession} did not pass sanity check. No further action."
        )

    # Register study and submit to ENA (if was not submitted before)
    registered_study = await process_study(
        study_accession,
        mgnify_study,
        run_accession,
        assembly_path,
        mgnify_run,
        upload_folder,
        dry_run,
    )
    # Create assembly manifest
    manifest = await prepare_assembly(
        run_accession,
        study_accession,
        mgnify_assembly,
        assembler,
        assembler_version,
        assembly_path,
        registered_study,
        upload_folder,
    )
    # Submit assembly with Webin-cli
    await submit_assembly_slurm(
        run_accession, manifest, mgnify_assembly, upload_folder, dry_run
    )
