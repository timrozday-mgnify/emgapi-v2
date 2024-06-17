import pytest

from workflows.flows.simple_example import github_stars


@pytest.mark.django_db
def test_prefect_simple_example_flow(prefect_harness, httpx_mock):
    httpx_mock.add_response(
        url="https://api.github.com/repos/EBI-Metagenomics/emg-viral-pipeline",
        json={"stargazers_count": 99},
    )
    httpx_mock.add_response(
        url="https://api.github.com/repos/EBI-Metagenomics/notebooks",
        json={"stargazers_count": 33},
    )
    stars = github_stars(
        ["EBI-Metagenomics/emg-viral-pipeline", "EBI-Metagenomics/notebooks"]
    )
    assert stars == [99, 33]
