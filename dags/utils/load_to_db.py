# load_to_db.py
import psycopg2

from utils.logger import get_logger

logger = get_logger(__name__.split(".")[-1])


def load_to_postgres(csv_path: str):
    """Loads company pageviews data into PostgreSQL."""
    conn = psycopg2.connect(
        host="localhost", user="dki", password="Sansbruit", dbname="company_pageviews"
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
    logger.success(f"Data successfully loaded into PostgreSQL from {csv_path}")
