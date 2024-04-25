import uuid

from django.db import models

from .signals import ready


class PrefectInitiatedHPCJob(models.Model):
    """
    A job initiated on the HPC cluster (AKA Slurm), from a Prefect flow run.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    prefect_flow_run_id = models.UUIDField(db_index=True)
    cluster_job_id = models.IntegerField(db_index=True, null=True, blank=True)
    command = models.TextField(null=True, blank=True)
    created = models.DateTimeField(auto_now_add=True)
    updated = models.DateTimeField(auto_now=True)
    metadata = models.JSONField(null=True, blank=True)

    class JobStates(models.TextChoices):
        PRE_CLUSTER_PENDING = "PREC", "Not yet submitted to cluster"
        CLUSTER_PENDING = "PEND", "Pending on cluster"
        CLUSTER_RUNNING = "RUN", "Running on cluster"
        COMPLETED_SUCCESS = "SUCC", "Completed in good state on cluster"
        COMPLETED_FAIL = "FAIL", "Completed in a failed state on cluster"

    last_known_state = models.CharField(
        choices=JobStates, default=JobStates.PRE_CLUSTER_PENDING, max_length=4
    )

    def __str__(self):
        return f"PrefectInitiatedHPCJob: {self.id} (flow-run: {self.prefect_flow_run_id} slurm: {self.cluster_job_id})"

    class Meta:
        verbose_name = "Prefect Initiated HPC Job"
        verbose_name_plural = "Prefect Initiated HPC Jobs"


ready()
