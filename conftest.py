import sys
from contextlib import contextmanager
from unittest import mock

import pymongo
import pytest
from unittest.mock import patch, AsyncMock
from prefect.testing.utilities import prefect_test_harness
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import analyses.models as mg_models
import ena.models as ena_models
from analyses.models import Assembler
from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyEMGBase,
    LegacyStudy,
    LegacyAnalysisJob,
    LegacySample,
)
from workflows.prefect_utils.slurm_flow import SlurmStatus


@pytest.fixture
def ena_study():
    return ena_models.Study.objects.create(accession="PRJ1", title="Project 1")


@pytest.fixture
def mgnify_study(ena_study):
    return mg_models.Study.objects.create(ena_study=ena_study, title="Project 1")


@pytest.fixture
def prefect_harness():
    with prefect_test_harness():
        yield


@pytest.fixture
def mock_suspend_flow_run(request):
    namespace = request.param
    with patch(f"{namespace}.suspend_flow_run", new_callable=AsyncMock) as mock_suspend:
        yield mock_suspend


@pytest.fixture
def mock_cluster_can_accept_jobs_yes():
    with patch(
        "workflows.prefect_utils.slurm_flow.cluster_can_accept_jobs"
    ) as mock_cluster_can_accept_jobs:
        mock_cluster_can_accept_jobs.return_value = 1000
        yield mock_cluster_can_accept_jobs


@pytest.fixture
def mock_start_cluster_job():
    with patch(
        "workflows.prefect_utils.slurm_flow.start_cluster_job"
    ) as mock_start_cluster_job:
        mock_start_cluster_job.side_effect = range(
            1, 1000
        )  # incrementing mocked slurm job ids
        yield mock_start_cluster_job


@pytest.fixture
def mock_check_cluster_job_all_completed():
    with patch(
        "workflows.prefect_utils.slurm_flow.check_cluster_job"
    ) as mock_check_cluster_job:
        mock_check_cluster_job.return_value = SlurmStatus.completed.value
        yield mock_check_cluster_job


@pytest.fixture
def top_level_biomes():
    mg_models.Biome.objects.create(path="root", biome_name="root")
    mg_models.Biome.objects.create(
        path="root.host_associated", biome_name="Host-Associated"
    )
    mg_models.Biome.objects.create(path="root.engineered", biome_name="Engineered")
    mg_models.Biome.objects.create(
        path="root.host_associated.human", biome_name="Human"
    )
    return mg_models.Biome.objects.all()


@pytest.fixture
def assemblers():
    for name, label in Assembler.NAME_CHOICES:
        mg_models.Assembler.objects.create(name=name, version="v0")
    return mg_models.Assembler.objects.all()


@pytest.fixture
def raw_read_ena_study():
    return ena_models.Study.objects.create(
        accession="PRJNA398089", title="Some Raw Reads"
    )


@pytest.fixture
def raw_reads_mgnify_study(raw_read_ena_study):
    return mg_models.Study.objects.create(
        ena_study=raw_read_ena_study, title=raw_read_ena_study.title
    )


@pytest.fixture
def assembly_study_accession():
    return "PRJNA567089"


@pytest.fixture
def raw_read_ena_sample(raw_read_ena_study):
    return ena_models.Sample.objects.create(
        study=raw_read_ena_study,
        metadata={"accession": "SAMN07793787", "description": "Sample 1"},
    )


@pytest.fixture
def raw_reads_mgnify_sample(raw_reads_mgnify_study, raw_read_ena_sample):
    return mg_models.Sample.objects.create(
        ena_sample=raw_read_ena_sample,
        ena_study=raw_reads_mgnify_study.ena_study,
    )


@pytest.fixture
def raw_read_run(raw_reads_mgnify_study, raw_reads_mgnify_sample):
    return mg_models.Run.objects.create(
        ena_accessions=["SRR6180434"],
        study=raw_reads_mgnify_study,
        ena_study=raw_reads_mgnify_study.ena_study,
        sample=raw_reads_mgnify_sample,
    )


@pytest.fixture
def mgnify_assembly_completed(
    mgnify_study, raw_read_run, raw_reads_mgnify_study, assemblers
):
    assembler = assemblers.filter(name=mg_models.Assembler.METASPADES).first()
    return mg_models.Assembly.objects.create(
        run=raw_read_run,
        reads_study=raw_reads_mgnify_study,
        ena_study=mgnify_study.ena_study,
        assembler=assembler,
        dir="slurm/fs/hps/tests/assembly_uploader",
        metadata={"coverage": 20},
        status={"status": "assembly_completed"},
    )


@pytest.fixture(scope="function")
def in_memory_legacy_emg_db():
    engine = create_engine("sqlite:///:memory:", echo=False)

    LegacyEMGBase.metadata.create_all(engine)

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    study = LegacyStudy(
        id=5000,
        centre_name="MARS",
        study_name="Bugs on mars",
        ext_study_id="ERP1",
        is_private=False,
        project_id="PRJ1",
        is_suppressed=False,
    )
    session.add(study)

    sample = LegacySample(
        sample_id=1000,
        ext_sample_id="ERS1",
        primary_accession="SAMEA1",
    )
    session.add(sample)

    analysis = LegacyAnalysisJob(
        job_id=12345,
        sample_id=1000,
        study_id=5000,
        pipeline_id=6,  # 6 is v6.0 in legacy EMG DB
        result_directory="some/dir/in/results",
        external_run_ids="ERR1000",
        secondary_accession="ERR1000",
        experiment_type_id=2,  # amplicon
        analysis_status_id=3,  # completed
    )
    session.add(analysis)
    session.commit()

    yield session

    session.close()
    engine.dispose()


@pytest.fixture(scope="function")
def mock_legacy_emg_db_session(in_memory_legacy_emg_db, monkeypatch):
    @contextmanager
    def mock_legacy_session():
        yield in_memory_legacy_emg_db

    monkeypatch.setattr(
        "workflows.data_io_utils.legacy_emg_dbs.legacy_emg_db_session",
        mock_legacy_session,
    )

    # forceful mocking of the session manager everywhere because monkeypatch doesn't catch already cached imports
    for module_name, module in sys.modules.items():
        if hasattr(module, "legacy_emg_db_session"):
            setattr(module, "legacy_emg_db_session", mock_legacy_session)


@pytest.fixture
def mock_mongo_client_for_taxonomy(monkeypatch):
    mock_mgya_data = {
        "accession": "MGYA00012345",
        "job_id": "12345",
        "pipeline_version": "5.0",
        "taxonomy_lsu": [
            {"count": 10, "organim": "Archaea:Euks::Something|5.0"},
            {"count": 20, "organim": "Bacteria|5.0"},
        ],
        "taxonomy_ssu": [
            {"count": 30, "organim": "Archaea:Euks::Something|5.0"},
            {"count": 40, "organim": "Bacteria|5.0"},
        ],
    }

    mock_client = mock.MagicMock()
    mock_db = mock.MagicMock()
    mock_collection = mock.MagicMock()

    mock_collection.find_one.return_value = mock_mgya_data
    mock_db.analysis_job_taxonomy = mock_collection
    mock_client.__getitem__.return_value = mock_db

    monkeypatch.setattr(pymongo, "MongoClient", lambda *args, **kwargs: mock_client)

    return mock_client
