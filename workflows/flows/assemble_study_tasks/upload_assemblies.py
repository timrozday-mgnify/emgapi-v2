from django.db.models import QuerySet
from prefect import flow
from prefect.task_runners import SequentialTaskRunner

import analyses.models
from workflows.flows.upload_assembly import upload_assembly


@flow(
    log_prints=True,
    task_runner=SequentialTaskRunner,
    flow_run_name="Upload assemblies of: {study}",
)
async def upload_assemblies(study: analyses.models.Study, dry_run: bool = False):
    """
    Uploads all completed, not-previously-uploaded assemblies to ENA.
    The first assembly upload will usually trigger a TPA study to be created.
    """
    assemblies_to_upload: QuerySet = study.assemblies_reads.filter_by_statuses(
        [analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED]
    ).exclude_by_statuses([analyses.models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED])
    print(f"Will upload assemblies: {assemblies_to_upload.acount()}")
    async for assembly in assemblies_to_upload:
        await upload_assembly(assembly.id, dry_run=dry_run)
