from prefect import flow
import django
django.setup()

from analyses.models import Analysis
from workflows.data_io_utils.mgnify_v6_utils.amplicon import EMG_CONFIG
from workflows.prefect_utils.slurm_flow import move_data


@flow(
    name="Copy Amplicon Pipeline Results",
    flow_run_name="Copy Amplicon Pipeline Results",
)
async def copy_amplicon_pipeline_results(analysis: Analysis):
    await move_data(
        analysis.results_dir,
        f"{EMG_CONFIG.slurm.ftp_results_dir}/{analysis.study.first_accession[:4]}/{analysis.study.first_accession}",
        "cp -r",
    )
