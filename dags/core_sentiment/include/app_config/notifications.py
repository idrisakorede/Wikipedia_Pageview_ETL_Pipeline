import logging

import requests
from airflow.utils.email import send_email
from core_sentiment.include.app_config.settings import config

logger = logging.getLogger(__name__)


def success_email(context):
    """Send success email notification."""
    if not config.ENABLE_EMAIL_ALERTS:
        return

    task_instance = context["task_instance"]
    subject = f"✅ Core Sentiment Pipeline - {task_instance.task_id} Succeeded"
    body = (
        f'Task: {task_instance.task_id}\n'
        f'Status: Success\n'
        f'Execution Date: {context["execution_date"]}\n'
        f'Log URL: {task_instance.log_url}\n'
    )
    # Use email_to_list for multiple recipients
    send_email(to=config.email_to_list, subject=subject, html_content=body)


def failure_email(context):
    """Send failure email notification."""
    if not config.ENABLE_EMAIL_ALERTS:
        return

    task_instance = context["task_instance"]
    subject = f"❌ Core Sentiment Pipeline - {task_instance.task_id} Failed"
    body = (
        f'Task: {task_instance.task_id}\n'
        f'Status: Failed\n'
        f'Execution Date: {context["execution_date"]}\n'
        f'Log URL: {task_instance.log_url}\n'
        f'Error: {context.get("exception")}\n'
    )
    # Use email_to_list for multiple recipients
    send_email(to=config.email_to_list, subject=subject, html_content=body)


def success_slack(context):
    """Send success Slack notification using webhook from env."""
    if not config.ENABLE_SLACK or not config.SLACK_WEBHOOK_URL:
        return

    try:
        ti = context["task_instance"]

        # Try to get metrics from XCom
        metrics = "N/A"
        try:
            load_result = ti.xcom_pull(task_ids="load_analytics.load_filtered_data")
            if load_result and isinstance(load_result, dict):
                metrics = f"Rows loaded: {load_result.get('rows_loaded', 'N/A')}"
        except (KeyError, AttributeError, TypeError) as e:
            logger.debug(f"Could not fetch metrics from XCom: {e}")
            metrics = "N/A"

        message = {
            "text": (
                f"✅ *SUCCESS*: Core Sentiment Pipeline\n"
                f"Task: `{ti.task_id}`\n"
                f"Date: {context['ds']}\n"
                f"Metrics: {metrics}\n"
            )
        }

        response = requests.post(config.SLACK_WEBHOOK_URL, json=message, timeout=10)
        response.raise_for_status()
        logger.info("Slack success notification sent")

    except requests.RequestException as e:
        logger.error(f"Failed to send Slack notification: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in Slack notification: {e}")


def failure_slack(context):
    """Send failure Slack notification using webhook from env."""
    if not config.ENABLE_SLACK or not config.SLACK_WEBHOOK_URL:
        return

    try:
        ti = context["task_instance"]
        error = str(context.get("exception", "Unknown error"))

        message = {
            "text": (
                f"❌ *FAILURE*: Core Sentiment Pipeline\n"
                f"Task: `{ti.task_id}`\n"
                f"Date: {context['ds']}\n"
                f"Error: {error}\n"
                f"Log: {ti.log_url}\n"
            )
        }

        response = requests.post(config.SLACK_WEBHOOK_URL, json=message, timeout=10)
        response.raise_for_status()
        logger.info("Slack failure notification sent")

    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")
