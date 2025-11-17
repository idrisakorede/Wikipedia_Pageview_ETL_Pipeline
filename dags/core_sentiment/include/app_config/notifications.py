import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests
from core_sentiment.include.app_config.settings import Settings

logger = logging.getLogger(__name__)


class Notifier:
    """Simple notifier for pipeline events using settings directly."""

    def __init__(self, config: Settings):
        """
        Initialize notifier with settings.

        Args:
            config: Settings instance with notification configuration
        """
        self.config = config

    def send_success(self, message: str, details: Optional[dict] = None):
        """Send success notification."""
        self._send(
            subject="✅ Pipeline Success",
            message=message,
            details=details,
            is_error=False,
        )

    def send_error(self, message: str, error: Optional[Exception] = None):
        """Send error notification."""
        details = (
            {"error": str(error), "error_type": type(error).__name__} if error else None
        )
        self._send(
            subject="❌ Pipeline Error", message=message, details=details, is_error=True
        )

    def _send(
        self, subject: str, message: str, details: Optional[dict], is_error: bool
    ):
        """Internal send method."""
        # Format details
        body = message
        if details:
            body += "\n\nDetails:\n"
            body += "\n".join(f"  {k}: {v}" for k, v in details.items())

        # Send via enabled channels
        if self.config.ENABLE_EMAIL_ALERTS:
            self._send_email(subject, body)

        if self.config.ENABLE_SLACK:
            self._send_slack(subject, body, is_error)

    def _send_email(self, subject: str, body: str):
        """Send email notification."""
        # Check if email is configured
        if not all(
            [
                self.config.SMTP_HOST,
                self.config.SMTP_USER,
                self.config.SMTP_PASSWORD,
                self.config.EMAIL_FROM,
                self.config.EMAIL_TO,
            ]
        ):
            logger.warning("Email not fully configured, skipping email notification")
            return

        try:
            msg = MIMEMultipart()
            msg["From"] = self.config.EMAIL_FROM
            msg["To"] = self.config.EMAIL_TO  # Single recipient
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(self.config.SMTP_HOST, self.config.SMTP_PORT) as server:
                server.starttls()
                server.login(self.config.SMTP_USER, self.config.SMTP_PASSWORD)
                server.send_message(msg)

            logger.info(f"Email sent: {subject}")

        except Exception as e:
            logger.error(f"Failed to send email: {e}")

    def _send_slack(self, subject: str, body: str, is_error: bool):
        """Send Slack notification."""
        if not self.config.SLACK_WEBHOOK_URL:
            logger.warning("Slack webhook not configured, skipping Slack notification")
            return

        try:
            color = "#ff0000" if is_error else "#36a64f"

            payload = {
                "attachments": [
                    {
                        "color": color,
                        "title": subject,
                        "text": body,
                        "footer": "Pipeline Monitor",
                        "ts": int(__import__("time").time()),
                    }
                ]
            }

            response = requests.post(
                self.config.SLACK_WEBHOOK_URL, json=payload, timeout=10
            )
            response.raise_for_status()

            logger.info(f"Slack notification sent: {subject}")

        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")


# Example usage
"""
from core_sentiment.include.app_config.settings import config
from core_sentiment.include.app_config.notifications import Notifier

notifier = Notifier(config)

try:
    result = your_pipeline_function()
    notifier.send_success(
        message="Pipeline completed successfully",
        details={"rows": 150, "output": "file.csv"}
    )
except Exception as e:
    notifier.send_error(
        message="Pipeline failed",
        error=e
    )
    raise
"""
