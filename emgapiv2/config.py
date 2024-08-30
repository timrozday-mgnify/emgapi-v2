from pydantic import BaseModel, AnyHttpUrl
from pydantic.networks import MongoDsn, MySQLDsn
from pydantic_settings import BaseSettings


class SlurmConfig(BaseModel):
    default_job_status_checks_limit: int = 10
    default_workdir: str = "/opt/jobs"
    pipelines_root_dir: str = "/app/workflows/pipelines"
    user: str = "root"

    incomplete_job_limit: int = 100
    # if this many jobs are RUNNING or PENDING, no more are submitted

    default_seconds_between_job_checks: int = 10
    # when a job is running, we wait this long between status checks

    default_seconds_between_submission_attempts: int = 10
    default_submission_attempts_limit: int = 100
    # if the cluster is "full", we wait this long before checking again for space,
    #   and only attempt submission a limited number of times before giving up.

    wait_seconds_between_slurm_flow_resumptions: int = 2

    job_log_tail_lines: int = 10
    # how many lines of slurm log to send to prefect each time we check it

    use_nextflow_tower: bool = True
    nextflow_tower_org: str = "EMBL"
    nextflow_tower_workspace: str = "ebi-spws-dev-microbiome-info"

    datamover_paritition: str = "datamover"

    assembly_uploader_python_executable: str = "python3"
    assembly_uploader_root_dir: str = ""
    webin_cli_executor: str = "/usr/bin/webin-cli/webin-cli.jar"

    amplicon_nextflow_master_job_memory: int = 5  # Gb


class AssemblerConfig(BaseModel):
    assembler_default: str = "metaspades"
    assembler_version_default: str = "3.15.3"


class WebinConfig(BaseModel):
    emg_webin_account: str = None
    emg_webin_password: str = None


class ENAConfig(BaseModel):
    primary_study_accession_re: str = "(PRJ[EDN][A-Z][0-9]+)"
    assembly_accession_re: str = "([EDS]RZ[0-9]{6,})"
    portal_search_api: AnyHttpUrl = "https://www.ebi.ac.uk/ena/portal/api/search"


class LegacyServiceConfig(BaseModel):
    emg_mongo_dsn: MongoDsn = "mongodb://mongo.not.here/db"
    emg_mongo_db: str = "emgapi"

    emg_mysql_dsn: MySQLDsn = "mysql+mysqlconnector://mysql.not.here/emg"

    emg_analysis_download_url_pattern: str = (
        "https://www.ebi.ac.uk/metagenomics/api/v1/analyses/{id}/file/{alias}"
    )


class EMGConfig(BaseSettings):
    slurm: SlurmConfig = SlurmConfig()
    environment: str = "development"
    webin: WebinConfig = WebinConfig()
    ena: ENAConfig = ENAConfig()
    assembler: AssemblerConfig = AssemblerConfig()
    legacy_service: LegacyServiceConfig = LegacyServiceConfig()

    model_config = {
        "env_prefix": "emg_",
        "env_nested_delimiter": "__",
    }
