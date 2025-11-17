import logging
from io import StringIO

import pandas as pd
import psycopg
from core_sentiment.include.app_config.settings import config
from pendulum import now

logger = logging.getLogger(__name__)


def load_filtered_pageviews_to_db(filtered_result: dict) -> dict:
    """
    Function:
        Load LLM-filtered pageview records into the curated database table.

    Args:
        filtered_result (dict):
            Dictionary that must contain key "csv_output" holding CSV text
            produced by the LLM.

    Returns:
        dict:
            {
                "rows_loaded": <int>,
                "status": "success" | "empty"
            }
    """

    try:
        logger.info("Loading filtered data to database...")

        # ------------------------------------------------------------------
        # Parse LLM CSV
        # ------------------------------------------------------------------
        csv_output = filtered_result.get("csv_output", "")
        if not csv_output:
            logger.warning("No filtered data to load")
            return {"rows_loaded": 0, "status": "empty"}

        df = pd.read_csv(StringIO(csv_output))
        logger.info(f"Filtered records: {len(df):,}")

        # ------------------------------------------------------------------
        # Add metadata columns
        # ------------------------------------------------------------------
        ts = now()
        df["filtered_at"] = ts
        df["processing_date"] = ts.date()
        df["filter_method"] = "llm_ollama_llama3.2:1b"

        # Postgres COPY needs no index
        df = df.reset_index(drop=True)

        # ------------------------------------------------------------------
        # Convert DataFrame → CSV buffer for COPY
        # ------------------------------------------------------------------
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)  # COPY handles columns
        buffer.seek(0)

        # ------------------------------------------------------------------
        # Connect with psycopg3 and COPY into table
        # ------------------------------------------------------------------
        conn_string = (
            f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}"
            f"@core_sentiment_db:5432/{config.DB_NAME}"
        )

        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # Build COPY command with column list
                columns = ", ".join(df.columns)
                copy_sql = f"""
                    COPY filtered_pageviews ({columns})
                    FROM STDIN WITH (FORMAT csv)
                """

                logger.info("Executing COPY INTO filtered_pageviews...")
                cur.copy(copy_sql, buffer)  # type: ignore[arg-type]
                conn.commit()

        logger.info(f"✓ Loaded {len(df):,} filtered records")

        return {
            "rows_loaded": len(df),
            "status": "success",
        }

    except Exception as e:
        logger.error(f"Failed to load filtered data: {e}")
        raise
