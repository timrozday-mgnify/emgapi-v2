from pydantic import BaseModel
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


class EMGConfig(BaseSettings):
    slurm: SlurmConfig = SlurmConfig()

    model_config = {
        "env_prefix": "emg_",
        "env_nested_delimiter": "__",
    }
