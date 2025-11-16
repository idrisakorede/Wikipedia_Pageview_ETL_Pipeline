import logging

import pandas as pd
import psycopg
from core_sentiment.include.app_config.settings import config
from pendulum import now
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def load_raw_pageviews_to_db(extract_info: dict) -> dict:
    """
    Function
        - Load ALL raw pageview data to warehouse.

        DATA FLOW:
            Extract CSV → Read ALL data → Add metadata → Load to raw_pageviews

        This function:
            1. Reads extracted CSV file
            2. Adds metadata columns (source_file, loaded_at, processing_date)
            3. Loads to raw_pageviews table using bulk insert
            4. Returns statistics

    Args:
        extract_info: Dictionary containing:
            - csv_path: Path to extracted CSV file
            - source_file: Original Wikipedia dump filename
            - Any other metadata from extraction

    Returns:
        Dictionary with load statistics:
            - rows_loaded: Number of records loaded
            - source_file: Source filename
            - csv_path: Path to CSV file
            - status: 'success' or 'failed'

    Raises:
        - FileNotFoundError: If CSV file doesn't exist
        - Exception: If database loading fails
    """
    try:
        csv_path = extract_info["csv_path"]
        source_file = extract_info["source_file"]

        logger.info(f"Loading raw data from: {csv_path}")

        # Count total rows
        total_rows = sum(1 for _ in open(csv_path)) - 1
        logger.info(f"Total records to load: {total_rows:,}")

        # Database connection string
        conn_string = (
            f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}"
            f"@core_sentiment_db:5432/{config.DB_NAME}"
        )

        current_time = now()
        chunk_size = 100_000
        chunks_processed = 0

        # Connect using psycopg3
        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # Process CSV in chunks
                for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
                    # Replace NaN with 'unknown'
                    chunk["domain"] = chunk["domain"].fillna("unknown")
                    chunk["page_title"] = chunk["page_title"].fillna("unknown")

                    # Add metadata
                    chunk["source_file"] = source_file
                    chunk["loaded_at"] = current_time
                    chunk["processing_date"] = current_time.date()

                    # Convert to CSV string for COPY
                    csv_buffer = chunk.to_csv(index=False, header=False)

                    # COPY directly from string
                    with cur.copy("""
                        COPY raw_pageviews (
                            domain,
                            page_title,
                            count_views,
                            source_file,
                            loaded_at,
                            processing_date
                        ) FROM STDIN WITH (FORMAT CSV)
                    """) as copy:
                        copy.write(csv_buffer)

                    chunks_processed += 1
                    rows_loaded = min(chunks_processed * chunk_size, total_rows)
                    logger.info(
                        f"Progress: {rows_loaded:,}/{total_rows:,} rows ({rows_loaded/total_rows*100:.1f}%)"
                    )

            # Commit transaction
            conn.commit()

        logger.info(f"✓ Successfully loaded {total_rows:,} records")

        return {
            "rows_loaded": total_rows,
            "source_file": source_file,
            "status": "success",
        }

    except Exception as e:
        logger.error(f"Failed to load raw data: {e}")
        raise


def verify_load(source_file: str) -> dict:
    """
    Function:
        - Verify that data was loaded successfully.

    Args:
        source_file: Source filename to verify

    Returns:
        Dictionary with verification results
    """
    try:
        conn_string = (
            f"postgresql+psycopg2://{config.DB_USER}:{config.DB_PASSWORD}"
            f"@core_sentiment_db:5432/{config.DB_NAME}"
        )
        engine = create_engine(conn_string)

        query = text("""
            SELECT 
                COUNT(*) AS record_count,
                MIN(loaded_at) AS load_time,
                COUNT(DISTINCT domain) AS domain_count,
                SUM(count_views) AS total_views
            FROM raw_pageviews
            WHERE source_file = :source_file
        """)

        with engine.connect() as conn:  # ✔ context manager OK
            result = conn.execute(query, {"source_file": source_file}).one()

        verification = {
            "source_file": source_file,
            "record_count": int(result.record_count or 0),
            "load_time": str(result.load_time) if result.load_time else None,
            "domain_count": int(result.domain_count or 0),
            "total_views": int(result.total_views or 0),
            "verified": True,
        }

        logger.info("Verification Results:")
        logger.info(f"  Source: {verification['source_file']}")
        logger.info(f"  Records: {verification['record_count']:,}")
        logger.info(f"  Loaded at: {verification['load_time']}")
        logger.info(f"  Total views: {verification['total_views']:,}")

        return verification

    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return {
            "source_file": source_file,
            "verified": False,
            "error": str(e),
        }
