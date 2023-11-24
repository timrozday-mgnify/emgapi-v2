import logging
import random
from dataclasses import dataclass
from typing import Optional


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


@dataclass
class db(Job):
    ...


@dataclass
class JobSubmitDescription:
    # https://pyslurm.github.io/23.2/reference/jobsubmitdescription/
    # TODO add other attributes as dataclass fields
    time_limit: str
    memory: str
    script: str
    name: Optional[str] = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args)

    def submit(self):
        job_id = random.randint(1, 1000000)
        logging.warning(
            f"Submitting slurm job {self.name}, but there is no functioning PySlurm implementation. Job ID will be random {job_id}"
        )
        return job_id
