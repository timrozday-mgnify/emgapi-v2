import uuid
from typing import Optional, Union

from django.db import models
from pydantic import BaseModel, ConfigDict, Field

from emgapiv2.model_utils import JSONFieldWithSchema

from .prefect_utils.slurm_status import SlurmStatus
from .signals import ready


class OrchestratedClusterJob(models.Model):

    class SlurmJobSubmitDescription(BaseModel):
        """
        Pydantic schema for storing a slurm JobSubmitDescription object as json.
        """

        model_config = ConfigDict(extra="allow")

        name: str = Field(..., description="Name of the job")
        script: str = Field(..., description="Content of the job script/command")
        memory_per_node: Optional[Union[str, int]] = Field(
            None, description="Memory required for the job"
        )
        time_limit: Optional[str] = Field(
            None, description="Wall time required for the job in HH:MM:SS format"
        )
        working_directory: Optional[str] = Field(
            None, description="Working directory for the job"
        )

    class JobInputFile(BaseModel):
        """
        Pydantic schema for storing the presence and content hash of an input file for the cluster job.
        Useful for determining if an input file has changed (hash diff).
        """

        path: str = Field(..., description="Path to the input file")
        hash: str = Field(..., description="Hash of the input file contents")
        hash_alg: str = Field(
            "blake2b", description="Hash algorithm used to hash the input file contents"
        )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    cluster_job_id = models.IntegerField(
        db_index=True
    )  # not pk because slurm recycles job IDs beyond a max int
    # N.B. this DOES NOT support array job IDs. Reasonable assumption for now since we do not have logic to launch these.
    flow_run_id = models.UUIDField(db_index=True)
    job_submit_description = JSONFieldWithSchema(schema=SlurmJobSubmitDescription)
    input_files_hashes = JSONFieldWithSchema(schema=JobInputFile, is_list=True)

    last_known_state = models.TextField(
        db_index=True,
        choices=SlurmStatus.as_choices(),
        null=True,
        blank=True,
        default=None,
    )
    state_checked_at = models.DateTimeField(null=True, blank=True, default=None)

    nextflow_trace = models.JSONField(default=list, null=True, blank=True)
    cluster_log = models.TextField(null=True, blank=True, default=None)


ready()
