from pydantic import BaseModel
from pydantic_settings import BaseSettings


class SlurmConfig(BaseModel):
    default_job_status_checks_limit: int = 10
    default_workdir: str = "/opt/jobs"
    pipelines_root_dir: str = "/app/workflows/pipelines"
    user: str = "root"
    incomplete_job_limit: int = (
        100  # max job count RUNNING + PENDING to allow more job submission
    )
    default_seconds_between_job_checks: int = 60
    wait_seconds_between_slurm_flow_resumptions: int = 2


class EMGConfig(BaseSettings):
    slurm: SlurmConfig = SlurmConfig()

    class Config:
        env_prefix = "emg_"
        env_nested_delimiter = "__"
