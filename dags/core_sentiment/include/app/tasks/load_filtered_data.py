import logging
from io import StringIO

import pandas as pd
from core_sentiment.include.app_config.settings import config
from pendulum import datetime
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def load_filtered_pageviews_to_db(filtered_result: dict) -> dict:
    """Load LLM-filtered data to curated layer."""
    try:
        logger.info("Loading filtered data to database...")

        # Parse CSV from LLM output
        csv_output = filtered_result.get("csv_output", "")
        if not csv_output:
            logger.warning("No filtered data to load")
            return {"rows_loaded": 0, "status": "empty"}

        df = pd.read_csv(StringIO(csv_output))
        logger.info(f"Filtered records: {len(df):,}")

        # Add metadata
        df["filtered_at"] = datetime.now()
        df["processing_date"] = datetime.now().date()
        df["filter_method"] = "llm_ollama_llama3.2:1b"

        # Load to database
        conn_string = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@core_sentiment_db:5432/{config.DB_NAME}"
        engine = create_engine(conn_string)

        df.to_sql(
            "filtered_pageviews",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
        )

        logger.info(f"✓ Loaded {len(df):,} filtered records")

        return {
            "rows_loaded": len(df),
            "status": "success",
        }

    except Exception as e:
        logger.error(f"Failed to load filtered data: {e}")
        raise
