# load_to_db.py
import os

import psycopg2
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

# Read environmental variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST", "core_sentiment_db")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5434")


def load_to_postgres(csv_path: str):
    """Loads company pageviews data into PostgreSQL."""
    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_pageviews (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            pageviews INTEGER,
            timestamp TIMESTAMP DEFAULT NOW()
        );
    """)

    with open(csv_path, "r") as f:
        cur.copy_expert(
            "COPY company_pageviews(name, pageviews) FROM STDIN WITH CSV HEADER", f
        )

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Data successfully loaded into PostgreSQL from {csv_path}")
