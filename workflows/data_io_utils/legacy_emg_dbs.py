from contextlib import contextmanager
from typing import Optional, List

import pymongo
from django.conf import settings
from prefect import task, get_run_logger
from sqlalchemy import Integer, String, Boolean, ForeignKey, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)


class LegacyEMGBase(DeclarativeBase):
    """
    Legacy models in EMG Mysql DB are defined with SQLAlechmy,
    so that we can use modern Django without worrying about
    no-longer-supported MySQL engines.

    These are just convenience ORM models, to help
    with importing legacy data into the v2 API's db.
    """

    pass


class LegacyBiome(LegacyEMGBase):
    __tablename__ = "BIOME_HIERARCHY_TREE"

    id: Mapped[int] = mapped_column("BIOME_ID", Integer, primary_key=True)
    lineage: Mapped[str] = mapped_column("LINEAGE", String)
    biome_name: Mapped[str] = mapped_column("BIOME_NAME", String)

    studies: Mapped["LegacyStudy"] = relationship("LegacyStudy", back_populates="biome")


class LegacyStudy(LegacyEMGBase):
    __tablename__ = "STUDY"

    id: Mapped[int] = mapped_column("STUDY_ID", Integer, primary_key=True)
    centre_name: Mapped[str] = mapped_column("CENTRE_NAME", String)
    study_name: Mapped[str] = mapped_column("STUDY_NAME", String)
    ext_study_id: Mapped[str] = mapped_column("EXT_STUDY_ID", String)
    project_id: Mapped[str] = mapped_column("PROJECT_ID", String)

    is_suppressed: Mapped[bool] = mapped_column("IS_SUPPRESSED", Boolean)
    is_private: Mapped[bool] = mapped_column("IS_PRIVATE", Boolean)

    analysis_jobs: Mapped[list["LegacyAnalysisJob"]] = relationship(
        back_populates="study"
    )

    biome_id: Mapped[int] = mapped_column(
        "BIOME_ID", ForeignKey("BIOME_HIERARCHY_TREE.BIOME_ID")
    )
    biome: Mapped["LegacyBiome"] = relationship("LegacyBiome", back_populates="studies")


class LegacySample(LegacyEMGBase):
    __tablename__ = "SAMPLE"

    sample_id: Mapped[int] = mapped_column("SAMPLE_ID", Integer, primary_key=True)
    ext_sample_id: Mapped[str] = mapped_column("EXT_SAMPLE_ID", String)
    primary_accession: Mapped[str] = mapped_column("PRIMARY_ACCESSION", String)

    analysis_jobs: Mapped[list["LegacyAnalysisJob"]] = relationship(
        back_populates="sample"
    )


class LegacyAnalysisJob(LegacyEMGBase):
    __tablename__ = "ANALYSIS_JOB"

    job_id: Mapped[int] = mapped_column("JOB_ID", Integer, primary_key=True)
    pipeline_id: Mapped[int] = mapped_column("PIPELINE_ID", Integer)
    analysis_status_id: Mapped[int] = mapped_column("ANALYSIS_STATUS_ID", Integer)
    result_directory: Mapped[str] = mapped_column("RESULT_DIRECTORY", String)
    external_run_ids: Mapped[str] = mapped_column("EXTERNAL_RUN_IDS", String)
    experiment_type_id: Mapped[int] = mapped_column("EXPERIMENT_TYPE_ID", Integer)
    secondary_accession: Mapped[str] = mapped_column("SECONDARY_ACCESSION", String)

    study_id: Mapped[int] = mapped_column("STUDY_ID", ForeignKey("STUDY.STUDY_ID"))
    study: Mapped["LegacyStudy"] = relationship(
        "LegacyStudy", back_populates="analysis_jobs"
    )

    sample_id: Mapped[int] = mapped_column("SAMPLE_ID", ForeignKey("SAMPLE.SAMPLE_ID"))
    sample: Mapped["LegacySample"] = relationship(
        "LegacySample", back_populates="analysis_jobs"
    )


@task(task_run_name="Get taxonomy for {mgya} from legacy mongo")
def get_taxonomy_from_api_v1_mongo(
    mgya: str,
) -> dict["analyses.models.Analysis.TaxonomySources", Optional[List]]:
    from analyses.models import Analysis  # prevent importing before apps ready

    logger = get_run_logger()
    mongo_dsn = settings.EMG_CONFIG.legacy_service.emg_mongo_dsn
    mongo_client = pymongo.MongoClient(mongo_dsn)
    db = mongo_client[settings.EMG_CONFIG.legacy_service.emg_mongo_db]

    taxonomies_collection: pymongo.collection.Collection = db.analysis_job_taxonomy

    mgya_taxonomies = taxonomies_collection.find_one({"accession": mgya})

    if not mgya_taxonomies:
        logger.warning(f"Did not find {mgya} in legacy mongo")
        return {}

    return {
        Analysis.TaxonomySources.SSU.value: mgya_taxonomies.get("taxonomy_ssu"),
        Analysis.TaxonomySources.LSU.value: mgya_taxonomies.get("taxonomy_lsu"),
        Analysis.TaxonomySources.ITS_ONE_DB.value: mgya_taxonomies.get("itsonedb"),
        Analysis.TaxonomySources.UNITE.value: mgya_taxonomies.get("unite"),
        Analysis.TaxonomySources.PR2.value: None,  # not implemented in v5 pipeline
    }


legacy_emg_engine = create_engine(settings.EMG_CONFIG.legacy_service.emg_mysql_dsn)
LegacyEmgSession = sessionmaker(bind=legacy_emg_engine)


@contextmanager
def legacy_emg_db_session():
    """
    A SQLAlchemy session for the legacy EMG database
    (which was MySQL in production).
    This was the DB behind the v1 EMG API.

    Example:
        with legacy_emg_db_session() as session:
            study_select_stmt = select(LegacyStudy).where(LegacyStudy.id == 100)
            legacy_study: LegacyStudy = session.scalar(study_select_stmt)
            ...
    """
    session = LegacyEmgSession()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
