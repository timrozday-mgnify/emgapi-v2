from typing import Union

from emgapiv2.enum_utils import DjangoChoicesCompatibleStrEnum


class SlurmStatus(DjangoChoicesCompatibleStrEnum):
    """
    Possible Slurm Job Statuses. Slurm v 23.
    """

    pending = "PENDING"
    running = "RUNNING"
    completing = "COMPLETING"
    completed = "COMPLETED"
    failed = "FAILED"
    terminated = "TERMINATED"
    suspended = "SUSPENDED"
    stopped = "STOPPED"
    timeout = "TIMEOUT"
    cancelled = "CANCELLED"
    out_of_memory = "OUT_OF_MEMORY"

    # Custom responses
    unknown = "UNKNOWN"


def slurm_status_is_okay(state: Union[SlurmStatus, str]):
    return state in [
        SlurmStatus.pending,
        SlurmStatus.running,
        SlurmStatus.unknown,
        SlurmStatus.completed,
    ]


def slurm_status_is_finished_successfully(state: Union[SlurmStatus, str]) -> bool:
    return state in [SlurmStatus.completed]


def slurm_status_is_finished_unsuccessfully(state: Union[SlurmStatus, str]) -> bool:
    return state in [
        SlurmStatus.failed,
        SlurmStatus.stopped,
        SlurmStatus.timeout,
        SlurmStatus.suspended,
        SlurmStatus.terminated,
        SlurmStatus.cancelled,
        SlurmStatus.out_of_memory,
    ]


def slurm_status_is_running(state: Union[SlurmStatus, str]) -> bool:
    return state in [SlurmStatus.running, SlurmStatus.completing]
