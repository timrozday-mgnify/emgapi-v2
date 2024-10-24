import os

from django.conf import settings
from prefect import get_run_logger, task
from prefect.runtime import flow_run
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_incoming_webhook_message


@task
async def notify_via_slack(message: str):
    logger = get_run_logger()

    try:
        slack_webhook = await SlackWebhook.load(
            settings.EMG_CONFIG.slack.slack_webhook_prefect_block_name
        )

        flow_run_name = flow_run.name
        prefect_ui_url = (
            os.getenv("PREFECT_API_URL", "https://example.org")
            .rstrip("/")
            .rstrip("api")
            .rstrip("/")
        )

        await send_incoming_webhook_message(
            slack_webhook=slack_webhook,
            text=message,
            slack_blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message,
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"From <{prefect_ui_url}/flow-runs/flow-run/{flow_run.id}|flow run {flow_run_name!r}>",
                    },
                },
            ],
        )
    except ValueError as e:
        logger.warning("Slack credentials not set up in prefect")
        logger.warning(e)
    except Exception as e:
        logger.warning("Failed to send slack notification.")
        logger.warning(e)
