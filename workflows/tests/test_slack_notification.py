from unittest.mock import patch

import pytest
from django.conf import settings
from prefect import flow
from prefect_slack import SlackWebhook

from workflows.prefect_utils.slack_notification import notify_via_slack


@flow
async def just_send_a_slack():
    await notify_via_slack("Hello world")


@pytest.mark.asyncio
@patch("workflows.prefect_utils.slack_notification.send_incoming_webhook_message")
async def test_slack_notifications_fails_gracefully(
    mock_send_incoming_webhook_message, prefect_harness
):
    # should fail gracefully if new slack credentials exist, and not send the message
    await just_send_a_slack()
    mock_send_incoming_webhook_message.assert_not_called()


@pytest.mark.asyncio
@patch("workflows.prefect_utils.slack_notification.send_incoming_webhook_message")
async def test_slack_notifications(mock_send_incoming_webhook_message, prefect_harness):
    await SlackWebhook(url="http://example.org/slack").save(
        settings.EMG_CONFIG.slack.slack_webhook_prefect_block_name
    )
    await just_send_a_slack()
    mock_send_incoming_webhook_message.assert_called_once()
