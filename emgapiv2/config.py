from pydantic import BaseModel, BaseSettings


class SlurmConfig(BaseModel):
    default_polling_interval_seconds: int = 5
    default_workdir: str = "/opt/jobs"
    pipelines_root_dir: str = "/app/workflows/pipelines"


class EMGConfig(BaseSettings):
    slurm: SlurmConfig = SlurmConfig()

    class Config:
        env_prefix = "emg_"
        env_nested_delimiter = "__"
