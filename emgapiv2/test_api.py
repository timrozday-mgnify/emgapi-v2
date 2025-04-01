import json
from typing import Callable, Optional, TypeVar, Union

import pytest
from ninja.testing import TestClient

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
    DownloadFileIndexFile,
)

R = TypeVar("R")


def call_endpoint_and_get_data(
    client: TestClient,
    endpoint: str,
    status_code: int = 200,
    count: Optional[int] = None,
    getter: Callable[[Union[dict, list]], R] = lambda response: response["items"],
) -> R:
    """
    Call an endpoint of the API. Check the status and response is expected.
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
    items = call_endpoint_and_get_data(ninja_api_client, "/studies/", count=1)
    assert items[0]["accession"] == raw_reads_mgnify_study.accession


@pytest.mark.django_db
def test_api_analyses_list(raw_read_analyses, ninja_api_client):
    items = call_endpoint_and_get_data(
        ninja_api_client, "/analyses/", count=len(raw_read_analyses)
    )
    assert items[0]["accession"] in [a.accession for a in raw_read_analyses]
    assert sorted([a["experiment_type"] for a in items]) == ["AMPLI", "METAG", "METAG"]


@pytest.mark.django_db
def test_api_study_analyses_list(raw_read_analyses, ninja_api_client):
    items = call_endpoint_and_get_data(
        ninja_api_client,
        f"/studies/{raw_read_analyses[0].study.accession}/analyses/",
        count=len(raw_read_analyses),
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


@pytest.mark.django_db
def test_api_analysis_downloads(raw_read_analyses, ninja_api_client):
    analysis = raw_read_analyses[0]
    dl = DownloadFile(
        alias="taxonomies-ssu.tsv.gz",
        short_description="Test file",
        file_type=DownloadFileType.TSV,
        download_group="taxonomies.closed_reference.ssu",
        download_type=DownloadType.TAXONOMIC_ANALYSIS,
        path="results/taxonomies.tsv.gz",
        long_description="This is a test file for taxonomies",
        file_size_bytes=1024,
        index_file=DownloadFileIndexFile(
            path="results/taxonomies.tsv.gz.gzi", index_type="gzi"
        ),
    )
    analysis.add_download(dl)
    analysis.refresh_from_db()
    api_analysis = call_endpoint_and_get_data(
        ninja_api_client,
        f"/analyses/{analysis.accession}",
        getter=lambda j: j,
    )

    assert api_analysis["accession"] == analysis.accession
    dl_api = next(
        d for d in api_analysis["downloads"] if d["alias"] == "taxonomies-ssu.tsv.gz"
    )
    print(json.dumps(dl_api, indent=2))
    assert (
        dl_api["url"]
        == "http://localhost:8080/app/data/tests/amplicon_v6_output/results/taxonomies.tsv.gz"
    )
    assert dl_api["index_file"]["relative_url"] == "taxonomies.tsv.gz.gzi"
    assert "path" not in dl_api
