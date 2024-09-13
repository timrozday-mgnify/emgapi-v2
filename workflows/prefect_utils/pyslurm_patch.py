import logging
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Job:
    # https://pyslurm.github.io/23.2/reference/job/#pyslurm.Job
    job_id: int

    exit_code: Optional[int] = None
    name: str = ""
    state: str = ""
    state_reason: str = ""

    def load(self, job_id: int):
        logging.warning(
            f"Loading slurm job {self.job_id}, but there is no functioning PySlurm implementation."
        )
        self.name = "Unknown job"
        self.state = "PENDING"
        self.state_reason = (
            "No PySlurm available, this is a non-functional implementation."
        )

    def cancel(self):
        logging.warning(
            f"Loading slurm job {self.job_id}, but there is no functioning PySlurm implementation."
        )
        self.state = "CANCELLED"
        self.state_reason = "None"
        return


@dataclass
class JobFilter:
    names: [str]
    users: [str]


@dataclass
class Jobs:
    @staticmethod
    def load(db_filter: JobFilter) -> List[Job]:
        return []


@dataclass
class db:
    Job = Job
    JobFilter = JobFilter
    Jobs = Jobs


# Global job ID incrementor


def job_id_incrementor():
    for i in range(1, int(1e6)):
        yield i


JOB_IDS = job_id_incrementor()


@dataclass
class JobSubmitDescription:
    # https://pyslurm.github.io/23.2/reference/jobsubmitdescription/
    time_limit: str
    memory_per_node: str
    script: str
    name: Optional[str] = ""
    working_directory: Optional[str] = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args)

    def submit(self):
        job_id = next(JOB_IDS)
        logging.warning(
            f"Submitting slurm job {self.name}, but there is no functioning PySlurm implementation. Job ID will be {job_id}"
        )
        return job_id
