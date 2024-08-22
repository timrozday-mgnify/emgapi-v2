import django
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from sqlalchemy import select

from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyStudy,
    LegacySample,
    get_taxonomy_from_api_v1_mongo,
    legacy_emg_db_session,
)

django.setup()

import ena.models
from analyses.models import Analysis, Study, Sample


@task
def make_study_from_legacy_emg_db(legacy_study: LegacyStudy) -> Study:
    logger = get_run_logger()

    ena_study, created = ena.models.Study.objects.get_or_create(
        accession=legacy_study.ext_study_id,
        defaults={
            "additional_accessions": [legacy_study.project_id],
            "title": legacy_study.study_name,
        },
    )
    if created:
        logger.warning(f"Created new ENA study object {ena_study}")

    mg_study, created = Study.objects.get_or_create(
        id=legacy_study.id,
        defaults={
            "ena_study": ena_study,
            "title": legacy_study.study_name,
            "ena_accessions": [legacy_study.ext_study_id, legacy_study.project_id],
            #     TODO: biome
        },
    )
    if created:
        logger.warning(f"Created new study object {mg_study}")
    return mg_study


@task
def make_sample_from_legacy_emg_db(legacy_sample: LegacySample, study: Study) -> Sample:
    logger = get_run_logger()

    ena_sample, created = ena.models.Sample.objects.get_or_create(
        accession=legacy_sample.primary_accession,
        defaults={
            "additional_accessions": [legacy_sample.ext_sample_id],
            "study": study.ena_study,
        },
    )
    if created:
        logger.warning(f"Created new ENA sample object {ena_sample}")

    mg_sample, created = Sample.objects.get_or_create(
        ena_sample=ena_sample,
        ena_study=study.ena_study,
        defaults={
            "ena_accessions": [
                legacy_sample.primary_accession,
                legacy_sample.ext_sample_id,
            ],
            #     TODO: biome
        },
    )
    if created:
        logger.warning(f"Created new sample object {mg_sample}")
    return mg_sample


@flow(
    name="Import V5 Amplicon Analyses",
    flow_run_name="Import V5 amplicon analyses from study: {mgys}",
    task_runner=SequentialTaskRunner,
)
def import_v5_amplicon_analyses(mgys: str):
    """
    This flow will iteratively import amplicon analyses (made with MGnify V5 pipeline)
    into the EMG DB.

    It connects to the legacy Mongo database server directly to copy data (it is big),
    but uses a TSV dump file of the legacy MySQL db (it is quite small).
    """
    logger = get_run_logger()

    study_id = int(mgys.upper().lstrip("MGYS"))

    with legacy_emg_db_session() as session:
        study_select_stmt = select(LegacyStudy).where(LegacyStudy.id == study_id)
        legacy_study: LegacyStudy = session.scalar(study_select_stmt)
        logger.info(f"Got legacy study {legacy_study}")

        study = make_study_from_legacy_emg_db(legacy_study)

        for legacy_analysis in legacy_study.analysis_jobs:
            legacy_sample = legacy_analysis.sample
            sample = make_sample_from_legacy_emg_db(legacy_sample, study)

            analysis, created = Analysis.objects.update_or_create(
                id=legacy_analysis.job_id,
                defaults={
                    "study": study,
                    "sample": sample,
                    "results_dir": legacy_analysis.result_directory,
                    "ena_study": study.ena_study,
                    "pipeline_version": Analysis.PipelineVersions.v5,
                },
            )

            if created:
                logger.info(f"Created analysis {analysis}")
            else:
                logger.warning(f"Updated analysis {analysis}")

            taxonomy = get_taxonomy_from_api_v1_mongo(analysis.accession)
            analysis.annotations[analysis.TAXONOMIES] = [
                {analysis.TAXONOMY_SOURCE: k.value, analysis.TAXONOMY_ASSIGNMENTS: v}
                for k, v in taxonomy.items()
            ]
            analysis.save()
