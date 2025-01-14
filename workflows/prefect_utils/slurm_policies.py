from datetime import timedelta
from operator import truth
from typing import Callable, List, Optional, Union

from pydantic import BaseModel, Field

from workflows.prefect_utils.slurm_status import (
    SlurmStatus,
    slurm_status_is_finished_unsuccessfully,
)


class _SlurmResubmitPolicy(BaseModel):
    policy_name: str = Field(..., description="Pretty name of this policy.")
    statuses_to_match: Union[List[SlurmStatus], Callable[[SlurmStatus], bool]] = Field(
        ...,
        description="If a job is submitted and an identical job exists in one of these statues, its resubmission will be controlled by this policy.",
    )
    ended_before: Optional[timedelta] = Field(
        ...,
        description="If a job ended earlier than this timedelta before now, its resubmission will be controlled by this policy.",
    )
    ended_after: Optional[timedelta] = Field(
        ...,
        description="If a job ended later than this timedelta before now, its resubmission will be controlled by this policy.",
    )

    resubmit: bool = Field(
        default=True,
        description="If True, jobs submitting following this policy will be resubmitted even if identical jobs already exist that match the policy criteria.",
    )


ResubmitIfFailedPolicy = _SlurmResubmitPolicy(
    policy_name="Resubmit if identical job previously failed",
    statuses_to_match=slurm_status_is_finished_unsuccessfully,
    resubmit=True,
)

ResubmitIfOlderThanAWeek = _SlurmResubmitPolicy(
    policy_name="Resubmit if identical job ended a week or more ago",
    statuses_to_match=truth,
    ended_before=timedelta(weeks=-1),
    resubmit=True,
)
