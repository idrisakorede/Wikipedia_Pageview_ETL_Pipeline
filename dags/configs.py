"""Centralized configuration management for the pipeline."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environmnet variables
load_dotenv()

# Base Directories
BASE_DIR = Path(os.getenv("DATA_DIR", "./data"))
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs"))
BASE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)


# Database Configuration
# class DBConfig(TypedDict):
#     host: str
#     port: int
#     dbname: str
#     user: str
#     password: str


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "company_pageviews"),
    "user": os.getenv("DB_USER", "airflow_user"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Wikipedia Data Source
WIKI_BASE_URL = os.getenv(
    "WIKI_BASE_URL", "https://dumps.wikimedia.org/other/pageviews/2025/2025-10/"
)

# Pipeline Configuration
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "300"))

# Email Configuration
EMAIL_CONFIG = {
    "smtp_host": os.getenv("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
    "smtp_user": os.getenv("SMTP_USER", ""),
    "smtp_password": os.getenv("SMTP_PASSWORD", ""),
    "alert_email": os.getenv("ALERT_EMAIL", ""),
}

ENABLE_EMAIL_ALERTS = os.getenv("ENABLE_EMAIL_ALERTS", "false").lower() == "true"
ENABLE_CLEANUP = os.getenv("ENABLE_CLEANUP", "true").lower() == "true"

# Company Mappings
COMPANY_NAMES = [
    "Google",
    "Apple_Inc.",
    "Amazon_(company)",
    "Facebook",
    "Meta_Platforms",
    "Microsoft",
]

COMPANY_ALIASES = {
    "apple_inc.": "apple",
    "meta_platforms": "facebook",
    "alphabet": "google",
    "amazon_(company)": "amazon",
    "facebook": "facebook",
    "google": "google",
    "microsoft": "microsoft",
}
