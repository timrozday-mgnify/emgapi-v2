from prefect import flow

from workflows.prefect_utils.slurm_flow import move_data


@flow(
    name="Copy Amplicon Pipeline Results",
    flow_run_name="Copy Amplicon Pipeline Results",
)
async def copy_amplicon_pipeline_results():
    await move_data(
        "/nfs/production/rdf/metagenomics/results",
        "/nfs/ftp/public/databases/metagenomics/mgnify_results",
        "cp -r",
    )
