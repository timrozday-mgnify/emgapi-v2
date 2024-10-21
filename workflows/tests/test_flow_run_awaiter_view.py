import uuid
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import pytest
from django.urls import reverse
from prefect.exceptions import FlowRunWaitTimeout


@pytest.mark.django_db(transaction=True)
@patch("workflows.views.wait_for_flow_run")
def test_wait_for_flowrun_view(
    mock_wait_for_flow_run, prefect_harness, admin_client, client
):
    mock_state = MagicMock()
    mock_state.is_final.return_value = False
    mock_state.is_completed.return_value = False

    mock_flowrun = MagicMock()
    mock_flowrun.id = uuid.uuid4()
    mock_flowrun.state = mock_state

    mock_wait_for_flow_run.side_effect = FlowRunWaitTimeout()
    mock_wait_for_flow_run.return_value = mock_flowrun

    # No auth - get redirect to admin login
    fetch_view_url = reverse(
        "workflows:wait_for_flowrun",
        kwargs={
            "flowrun_id": str(mock_flowrun.id),
            "next_url": quote(reverse("admin:analyses_sample_changelist"), safe=""),
        },
    )
    response = client.get(fetch_view_url)
    assert response.status_code == 302
    assert "admin/login" in response.url
    mock_wait_for_flow_run.assert_not_called()

    # As admin, should see flow run still running
    response = admin_client.get(fetch_view_url)
    mock_wait_for_flow_run.assert_called()
    assert response.status_code == 200
    assert f"Waiting for flowrun {mock_flowrun.id}" in response.content.decode("utf-8")

    # flow run completes
    mock_flowrun.state.is_completed.return_value = True
    mock_flowrun.state.is_final.return_value = True
    mock_wait_for_flow_run.side_effect = None

    response = admin_client.get(fetch_view_url)
    assert response.status_code == 302
    assert "admin/analyses/sample" in response.url
