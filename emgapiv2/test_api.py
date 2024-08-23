import pytest


@pytest.mark.django_db
def test_api_study(mgnify_study, ninja_api_client):
    response = ninja_api_client.get("/studies")
    assert response.status_code == 200

    json_response = response.json()
    items = json_response["items"]
    assert len(items) == 1
    assert items[0]["accession"] == mgnify_study.accession
    assert json_response["count"] == 1
