from datetime import timedelta
from operator import truth
from typing import Callable, List, Optional, Union

from pydantic import BaseModel, Field

from workflows.models import OrchestratedClusterJob
from workflows.prefect_utils.slurm_status import (
    SlurmStatus,
    slurm_status_is_finished_unsuccessfully,
)


def job_submissions_are_identical(
    a: OrchestratedClusterJob, b: OrchestratedClusterJob
) -> bool:
    return a.job_submit_description == b.job_submit_description


class _SlurmResubmitPolicy(BaseModel):
    policy_name: str = Field(..., description="Pretty name of this policy.")
    comparator: Callable[[OrchestratedClusterJob, OrchestratedClusterJob], bool] = (
        Field(
            default=job_submissions_are_identical,
            description="Comparator function to determine if the submitted jobs of two OrchestratedClusterJobs are identical.",
        )
    )
    if_status_matches: Union[List[SlurmStatus], Callable[[SlurmStatus], bool]] = Field(
        ...,
        description="If a job is submitted and an identical job exists in one of these statues, its resubmission will be controlled by this policy.",
    )
    if_ended_before: Optional[timedelta] = Field(
        ...,
        description="If a job ended earlier than this timedelta before now, its resubmission will be controlled by this policy.",
    )
    if_ended_after: Optional[timedelta] = Field(
        ...,
        description="If a job ended later than this timedelta before now, its resubmission will be controlled by this policy.",
    )
    then_resubmit: bool = Field(
        default=True,
        description="If True, jobs submitting following this policy will be resubmitted even if identical jobs already exist that match the policy criteria.",
    )


ResubmitIfFailedPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit if identical job previously failed",
    if_status_matches=slurm_status_is_finished_unsuccessfully,
    then_resubmit=True,
)

ResubmitIfOlderThanAWeekPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit if identical job ended a week or more ago",
    if_status_matches=truth,
    if_ended_before=timedelta(weeks=-1),
    then_resubmit=True,
)

ResubmitAlwaysPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit regardless of any previous jobs",
    if_status_matches=truth,
    then_resubmit=True,
)

NeverResubmitPolicy = _SlurmResubmitPolicy(
    policy_name="Never resubmit if a previous identical job exists",
    if_status_matches=truth,
    then_resubmit=False,
)
