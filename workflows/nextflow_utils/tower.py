import logging
from typing import Optional

from django.conf import settings
from pydantic import AnyUrl
from pydantic_core import Url


def maybe_get_nextflow_tower_browse_url(command: str) -> Optional[AnyUrl]:
    """
    If the command looks like a nextflow run with tower enabled and an explicitly defined name,
    return the Nextflow Tower URL for it (to be browsed).
    :param command: A command-line instruction e.g. nextflow run....
    :return: A Nextflow Tower / Seqera Platform URL, or None
    """
    if "nextflow run" in command and "-tower" in command and "-name" in command:
        try:
            wf_name = command.split("-name")[1].strip().split(" ")[0]
        except KeyError:
            logging.warning(
                f"Could not determine nextflow workflow run name from {command}"
            )
            return
        return Url(
            f"https://cloud.seqera.io/orgs/{settings.EMG_CONFIG.slurm.nextflow_tower_org}/workspaces/{settings.EMG_CONFIG.slurm.nextflow_tower_workspace}/watch?search={wf_name}"
        )
