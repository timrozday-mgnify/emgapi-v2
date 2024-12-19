from typing import Callable, Optional, TypeVar, Union

import pytest
from ninja.testing import TestClient

R = TypeVar("R")


def call_endpoint_and_get_data(
    client: TestClient,
    endpoint: str,
    status_code: int = 200,
    count: Optional[int] = None,
    getter: Callable[[Union[dict, list]], R] = lambda response: response["items"],
) -> R:
    """
    Call an endpoint of the API. Check the sat
    :param status_code: Expected value for status code, e.g. 200
    :param count: Expected value for `count` property at top level, if expected (e.g. a list endpoint)
    :param getter: Function/lambda that takes the API response JSON dict/list and return data you want
    :param client: Ninja API client fixture
    :param endpoint: path from APIP root, e.g. "/studies"
    :return: the API response passed through getter
    """
    response = client.get(endpoint)
    assert response.status_code == status_code
    j = response.json()
    if count is not None:
        assert j.get("count") == count
    return getter(j)


@pytest.mark.django_db
def test_api_study(raw_reads_mgnify_study, ninja_api_client):
    items = call_endpoint_and_get_data(ninja_api_client, "/studies", count=1)
    assert items[0]["accession"] == raw_reads_mgnify_study.accession


@pytest.mark.django_db
def test_api_analyses_list(raw_read_analyses, ninja_api_client):
    items = call_endpoint_and_get_data(
        ninja_api_client, "/analyses", count=len(raw_read_analyses)
    )
    assert items[0]["accession"] in [a.accession for a in raw_read_analyses]
    assert sorted([a["experiment_type"] for a in items]) == ["AMPLI", "METAG", "METAG"]


@pytest.mark.django_db
def test_api_analysis_detail(raw_read_analyses, ninja_api_client):
    analysis = call_endpoint_and_get_data(
        ninja_api_client,
        f"/analyses/{raw_read_analyses[0].accession}",
        getter=lambda j: j,
    )
    assert analysis["accession"] == raw_read_analyses[0].accession
    assert analysis["study_accession"] == raw_read_analyses[0].study.accession
    assert (
        analysis["quality_control_summary"]["before_filtering"]["total_reads"] == 66124
    )
