from datetime import timedelta
import os
import gzip
import re
from Bio import SeqIO

from prefect import flow, task

from prefect_shell import ShellOperation
import django
from django.conf import settings
from prefect.input import RunInput
from prefect.task_runners import SequentialTaskRunner
from prefect_shell import shell_run_command

from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.slurm_flow import (
    run_cluster_jobs,
    slurm_status_is_finished_successfully,
    FINAL_SLURM_STATE,
    SLURM_JOB_ID,
)
from emgapiv2.settings import EMG_CONFIG

OPTIONAL_SPADES_FILES = [
    ".assembly_graph.fastg.gz",
    ".assembly_graph_with_scaffolds.gfa.gz",
    ".scaffolds.fa.gz"
]
SPADES_PARAMS = "params.txt"

# TODO add values to config
PATH_TO_WEBIN_CLI = "webincli/webin-cli-7.2.1.jar"

@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Sanity check: {accession}",
    log_prints=True,
)
def check_assembly(accession, assembly_path, assembler):
    print(f"Check assembly {accession}")
    # check filesize
    if not os.path.exists(assembly_path):
        print(f"{assembly_path} does not exist")
        return False
    file_size = os.path.getsize(assembly_path)
    if not file_size:
        print(f"Contigs file is empty for assembly {accession}")
        return False
    # check spades files
    if 'spades' in assembler:
        for postfix in OPTIONAL_SPADES_FILES:
            if not os.path.exists(os.path.join(os.path.dirname(assembly_path), accession + postfix)):
                print(f"WARN: {accession + postfix} does not exist")
        if not os.path.exists(os.path.join(os.path.dirname(assembly_path), SPADES_PARAMS)):
            print(f"WARN: {SPADES_PARAMS} does not exist")

    # check number of contigs
    # TODO if we gonna have assembly_job.result.num_contigs record in DB - reuse it
    count = 0
    with gzip.open(assembly_path, 'rt') as handle:
        for _ in SeqIO.parse(handle, "fasta"):
            count += 1
            if count >= 2:
                return True
    if count < 2:
        print(f"Number of contigs in assembly file {assembly_path} is less than 2")
        return False
    return True


@task(
    retries=2,
    #cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="run shell command {command}",
    log_prints=True,
)
async def run_small_shell_command(command, env=None):
    cmd = ShellOperation(commands=[command], env=env, return_all=True)
    result = await cmd.run()
    # ShellOperation returns a list of outputs
    if result:
        stdout = result[0] if result[0] else "No stdout"
        stderr = result[1] if len(result) > 1 else "No stderr"
        print(f"stdout: {stdout}")
        print(f"stderr: {stderr}")
        return stdout, stderr
    else:
        print("No result from ShellOperation")
        return None, None


@task(
    retries=2,
    #cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Create study XML: {study_accession}",
    log_prints=True,
)
async def create_study_xml(study_accession, library, output_dir):
    if EMG_CONFIG.slurm.assembly_uploader_root_dir:
        # take from root_dir installation
        launcher = f"python3 {os.path.join(EMG_CONFIG.slurm.assembly_uploader_root_dir, 'assembly_uploader', 'study_xmls.py')}"
    else:
        # take from env installation
        launcher = "study_xmls"
    command = \
        f"{launcher} " \
        f"--study {study_accession} " \
        f"--library {library} " \
        f"--output-dir {output_dir} " \
        f"--center EMG "
    try:
         await run_small_shell_command(command,
                                      env={
                                          "ENA_WEBIN": EMG_CONFIG.webin.emg_webin_account,
                                          "ENA_WEBIN_PASSWORD": EMG_CONFIG.webin.emg_webin_password
                                      })
    except Exception as e:
        print(f"Command failed with error: {e}")
    # check was upload folder created or not
    upload_folder = os.path.join(output_dir, study_accession + '_upload')
    if os.path.exists(upload_folder):
        if len([i for i in os.listdir(upload_folder) if i.endswith('.xml')]) == 2:
            return upload_folder
    return None


@task(
    retries=2,
    #cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Submit study XML: {study_accession}",
    log_prints=True,
)
async def submit_study_xml(study_accession, upload_dir, dry_run):
    if EMG_CONFIG.slurm.assembly_uploader_root_dir:
        # take from root_dir installation
        launcher = f"python3 {os.path.join(EMG_CONFIG.slurm.assembly_uploader_root_dir, 'assembly_uploader', 'submit_study.py')}"
    else:
        # take from env installation
        launcher = "submit_study"
    command = \
        f"{launcher} " \
        f"--study {study_accession} " \
        f"--directory {upload_dir} "
    if dry_run:
        command += "--test "
    try:
        _, output_log = await run_small_shell_command(command,
                                                      env={
                                                          "ENA_WEBIN": EMG_CONFIG.webin.emg_webin_account,
                                                          "ENA_WEBIN_PASSWORD": EMG_CONFIG.webin.emg_webin_password
                                                      })
        print('LOG', output_log)
        # get a registered study accession from logs
        if 'Make a note of this!' in output_log or 'An accession with this alias already exists in project' in output_log:
            new_acc = re.findall(rf"{EMG_CONFIG.ena.primary_study_accession_re}", output_log)
            if new_acc:
                return new_acc[0]

    except Exception as e:
        print(f"Command failed with error: {e}")
    return False


@task(
    retries=2,
    #cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Generate csv for upload: {assembly_accession}",
    log_prints=True,
)
def generate_assembly_csv(metadata_dir, assembly_accession, coverage, assembler, assembler_version, assembly_path):
    assembly_csv = os.path.join(metadata_dir, assembly_accession + '.csv')
    with open(assembly_csv, 'w') as file_out:
        file_out.write(','.join(['Run', 'Coverage', 'Assembler', 'Version', 'Filepath']) + '\n')
        line = ','.join([assembly_accession, str(coverage), assembler, assembler_version, assembly_path])
        file_out.write(line + '\n')
    return assembly_csv


@task(
    retries=2,
    #cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Generate assembly manifest: {assembly_accession}",
    log_prints=True,
)
async def generate_assembly_xml(study_accession, assembly_accession, data_csv_path, registered_study, upload_dir):
    if EMG_CONFIG.slurm.assembly_uploader_root_dir:
        # take from root_dir installation
        launcher = f"python3 {os.path.join(EMG_CONFIG.slurm.assembly_uploader_root_dir, 'assembly_uploader', 'assembly_manifest.py')}"
    else:
        # take from env installation
        launcher = "assembly_manifest"
    command = \
        f"{launcher} " \
        f"--study {study_accession} " \
        f"--data {data_csv_path} " \
        f"--assembly_study {registered_study} " \
        f"--output-dir {upload_dir} "
    try:
        await run_small_shell_command(command,
                                      env={
                                              "ENA_WEBIN": EMG_CONFIG.webin.emg_webin_account,
                                              "ENA_WEBIN_PASSWORD": EMG_CONFIG.webin.emg_webin_password
                                          })
        return True
    except Exception as e:
        print(f"Command failed with error: {e}")
        return False


@flow(
    name="Sanity check and upload an assembly",
    log_prints=True,
    flow_run_name="Sanity check and upload",
    task_runner=SequentialTaskRunner,
)
async def assembly_uploader():
    """ """
    # get path to assembly contigs file
    # TODO replace with real assembly from DB
    study = "ERP108082"
    library = "metagenome"
    assembly = "SRR3960572"
    assembly_path = "assembly_uploader/test/test.fasta.gz"
    dry_run = True
    coverage = "20.0"
    assembler = "metaspades"
    assembler_version = "3.15.3"


    #assembly = analyses.models.Assembly.objects.get(id=assembler_id)
    print(f"Processing assembly: {assembly}")
    if check_assembly(assembly, assembly_path, assembler):
        print(f"{assembly} passed sanity check")
    else:
        print(f"{assembly} did not pass sanity check. No further action.")
        return

    # TODO: check a first completed assembly and register study only once
    print(f"Register study: {study}")
    upload_output_folder = os.path.dirname(assembly_path)
    upload_folder = await create_study_xml(study_accession=study, library=library, output_dir=upload_output_folder)
    if upload_folder:
        print(f"Upload folder {upload_folder} and study XMLs were created")
    else:
        print("Error occurred on study XML creation step. No further action.")
        return

    print(f"Submit study: {study}")
    registered_study = await submit_study_xml(study_accession=study, upload_dir=upload_folder, dry_run=dry_run)
    if registered_study:
        print(f"Study submitted successfully under {registered_study}")
    else:
        print("Study submission failed")

    metadata_dir = os.path.join(upload_folder, "metadata")
    if not os.path.exists(metadata_dir):
        os.mkdir(metadata_dir)
    print(f"Generate csv for assembly {assembly}")
    data_csv_path = generate_assembly_csv(
        metadata_dir=metadata_dir,
        assembly_accession=assembly,
        coverage=coverage,
        assembler=assembler,
        assembler_version=assembler_version,
        assembly_path=assembly_path
    )

    print("Generate assembly manifests")
    await generate_assembly_xml(
        study_accession=study,
        assembly_accession=assembly,
        data_csv_path=data_csv_path,
        registered_study=registered_study,
        upload_dir=upload_output_folder
    )
    # run webin-cli job
    manifest = os.path.join(upload_folder, assembly + '.manifest')
    if EMG_CONFIG.slurm.webin_cli_executor:
        # take from root_dir installation
        launcher = EMG_CONFIG.slurm.webin_cli_executor
    else:
        # take from env installation
        launcher = "ena-webin-cli"
    command = f"{launcher} " \
              f"-context=genome " \
              f"-manifest={manifest} " \
              f"-userName={EMG_CONFIG.webin.emg_webin_account} " \
              f"-password={EMG_CONFIG.webin.emg_webin_password} " \
              f"-submit "
    if dry_run:
        command += "-test "
    command = "ena-webin-cli -h"
    slurm_job_results = await run_cluster_jobs(
        names=[
            f"Upload assembly {assembly} to ENA"
        ],
        commands=[command],
        expected_time=timedelta(hours=1),
        memory=f"500M",
        environment="ALL",  # copy env vars from the prefect agent into the slurm job
        raise_on_job_failure=False,  # allows some jobs to fail without failing everything
    )

    if slurm_status_is_finished_successfully(slurm_job_results[0][FINAL_SLURM_STATE]):
        print(f"Successfully ran webin-cli upload for {assembly}")
    else:
        print(
            f"Something went wrong running webin-cli upload for {assembly} in job {slurm_job_results[0][SLURM_JOB_ID]}"
        )

# TODO add input assembly_id, assembler, dry_run
if __name__ == "__main__":
    assembly_uploader()
