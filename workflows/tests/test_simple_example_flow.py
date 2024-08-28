import logging

import pytest

from workflows.flows.simple_example import github_stars
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@pytest.mark.django_db(transaction=True)
def test_prefect_simple_example_flow(prefect_harness, httpx_mock):
    httpx_mock.add_response(
        url="https://api.github.com/repos/EBI-Metagenomics/emg-viral-pipeline",
        json={"stargazers_count": 99},
    )
    httpx_mock.add_response(
        url="https://api.github.com/repos/EBI-Metagenomics/notebooks",
        json={"stargazers_count": 33},
    )

    logger = logging.getLogger("prefect")
    logger.propagate = True

    stars_flow_run = run_flow_and_capture_logs(
        github_stars,
        ["EBI-Metagenomics/emg-viral-pipeline", "EBI-Metagenomics/notebooks"],
    )

    assert "stars" in stars_flow_run.logs
    assert stars_flow_run.result == [99, 33]
