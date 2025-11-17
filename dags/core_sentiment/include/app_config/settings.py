from typing import List

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load environment variables from the .env file into the runtime environment
load_dotenv()


# Define a class that reads environment variables and validates them
class Settings(BaseSettings):
    RAW_PAGEVIEWS_DIR: str
    PROCESSED_PAGEVIEWS_DIR: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    # Ollama configuration with defaults
    OLLAMA_HOST: str
    OLLAMA_MODEL: str
    OLLAMA_TIMEOUT: int

    # Email settings
    ENABLE_EMAIL_ALERTS: bool = False
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    EMAIL_FROM: str = ""
    EMAIL_TO: str = ""  # Comma-separated

    # Slack settings
    ENABLE_SLACK: bool = False
    SLACK_WEBHOOK_URL: str = ""

    @property
    def email_to_list(self) -> List[str]:
        return [e.strip() for e in self.EMAIL_TO.split(",") if e.strip()]

    # Configuration for how environment variables are loaded and handled
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )


# Creates a single instance to access configuration values throughout
config = Settings()  # type: ignore[call-arg]
