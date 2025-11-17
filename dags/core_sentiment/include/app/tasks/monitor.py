import logging
from typing import Optional

from core_sentiment.include.app_config.notifications import Notifier
from core_sentiment.include.app_config.settings import config

logger = logging.getLogger(__name__)


class NotificationManager:
    """Singleton manager for pipeline notifications."""

    _instance: Optional["NotificationManager"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Use settings
        self.notifier = Notifier(config)
        self._initialized = True


# Global instance
notifier_manager = NotificationManager()


def send_pipeline_success(**context):
    """
    Airflow callback for pipeline success.
    Gathers metrics from XCom and sends notification.
    """
    ti = context["ti"]

    try:
        # Gather results from XCom
        load_raw_result = ti.xcom_pull(
            task_ids="extract_load_raw.load_raw_to_warehouse"
        )
        load_filtered_result = ti.xcom_pull(
            task_ids="load_analytics.load_filtered_data"
        )

        # Build summary
        details = {
            "processing_date": str(context.get("ds", "unknown")),
            "raw_rows_loaded": load_raw_result.get("rows_inserted", 0)
            if load_raw_result
            else 0,
            "filtered_rows_loaded": load_filtered_result.get("rows_loaded", 0)
            if load_filtered_result
            else 0,
            "dag_run_id": context.get("run_id", "unknown"),
        }

        notifier_manager.notifier.send_success(
            message="✅ Core Sentiment Pipeline completed successfully", details=details
        )

        logger.info("Success notification sent")

    except Exception as e:
        logger.warning(f"Could not send success notification: {e}")


def send_pipeline_failure(context):
    """
    Airflow callback for pipeline failure.
    Sends error notification with context.
    """
    try:
        exception = context.get("exception")
        task_instance = context.get("task_instance")

        notifier_manager.notifier.send_error(
            message=f"❌ Core Sentiment Pipeline failed at task: {task_instance.task_id if task_instance else 'unknown'}",
            error=exception,
        )

        logger.info("Failure notification sent")

    except Exception as e:
        logger.error(f"Could not send failure notification: {e}")
