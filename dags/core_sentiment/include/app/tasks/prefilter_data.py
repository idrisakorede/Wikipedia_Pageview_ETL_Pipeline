import logging
from pathlib import Path

import pandas as pd
import psycopg
from core_sentiment.include.app_config.settings import config
from pendulum import now
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class PrefilterError(Exception):
    """Custom exception for prefilter errors"""

    pass


def prefilter_from_db(
    min_views: int = 100,
    limit: int = 5000,
    processing_date: str = "CURRENT_DATE",
) -> str:
    """
    Function:
        - Pre-filter pageview data from warehouse for LLM processing.

        DATA FLOW:
            raw_pageviews (warehouse) → SQL query with filters → temp CSV → LLM

        This function:
            1. Queries raw_pageviews table (NOT CSV files)
            2. Applies basic business rules (thresholds, filters)
            3. Exports to temp CSV (only for LLM processing)
            4. Returns path to temp CSV

    Args:
        - min_views: Minimum pageview threshold (default: 100)
        - limit: Maximum records to return (default: 5000)
        - processing_date: SQL date expression (default: 'CURRENT_DATE')

    Returns:
        - Path to temporary CSV file with pre-filtered data

    Raises:
        - Exception: If database query or file writing fails
    """
    try:
        logger.info("=" * 60)
        logger.info("PRE-FILTERING FROM WAREHOUSE")
        logger.info("=" * 60)
        logger.info("Source: raw_pageviews table (warehouse)")
        logger.info(f"Minimum views: {min_views}")
        logger.info(f"Limit: {limit}")
        logger.info(f"Processing date: {processing_date}")

        # Create database connection
        conn_string = (
            f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}"
            f"@core_sentiment_db:5432/{config.DB_NAME}"
        )

        # Query warehouse with psycopg3
        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # Check the domains in the warehouse
                cur.execute("SELECT ")

                query = """
                    SELECT 
                        domain,
                        page_title,
                        count_views
                    FROM raw_pageviews
                    WHERE processing_date = CURRENT_DATE
                        AND count_views >= %s
                    ORDER BY count_views DESC
                    LIMIT %s
                """

                logger.debug(f"Query: {query}")
                logger.debug(
                    f"Parameters: processing_date={processing_date}, min_views={min_views}, limit={limit}"
                )

                # Execute with parameters
                cur.execute(query, (min_views, limit))

                # Fetch result
                rows = cur.fetchall()

                # Ensure metadata exists
                if cur.description is None:
                    raise RuntimeError(
                        "Query returned no column metadata. Ensure the query is a SELECT."
                    )

                # Check if we have results
                if not rows:
                    logger.warning("No records match pre-filter criteria")
                    df = pd.DataFrame(columns=["domain", "page_title", "count_views"])
                else:
                    # Get column names from description
                    columns = [desc[0] for desc in cur.description]

                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=columns)

                    logger.info(f"Retrieved {len(df):,} records from warehouse")

        if len(df) == 0:
            logger.warning("No records match pre-filter criteria")
            raise ValueError("Pre-filtering returned no results from warehouse")

        # Apply additional business logic filters
        logger.info("Applying business rule filters...")
        initial_count = len(df)
        df = apply_business_rule_filters(df)
        removed = initial_count - len(df)

        logger.info(f"Business rules removed {removed:,} records")
        logger.info(f"After all filters: {len(df):,} records")

        if len(df) == 0:
            raise ValueError("All records filtered out by business rules")

        # Save to temp CSV (only for LLM processing)
        temp_dir = Path(config.PROCESSED_PAGEVIEWS_DIR)
        temp_dir.mkdir(parents=True, exist_ok=True)

        timestamp = now().strftime("%Y%m%d_%H%M%S")
        csv_path = temp_dir / f"prefiltered_{timestamp}.csv"

        df.to_csv(csv_path, index=False)
        logger.info(f"Saved temp CSV for LLM: {csv_path}")

        # Log sample of data
        logger.info("Sample of pre-filtered pages:")
        for idx, row in enumerate(df.head(10).itertuples(), start=1):
            logger.info(f"  {idx}. {row.page_title}: {row.count_views:,} views")

        logger.info("=" * 60)
        logger.info(f"✓ Pre-filtering complete: {len(df):,} records ready for LLM")
        logger.info("=" * 60)

        return str(csv_path)

    except ValueError as e:
        logger.error(f"Pre-filtering validation error: {e}")
        raise

    except Exception as e:
        logger.error(f"Failed to pre-filter from warehouse: {e}")
        logger.exception("Full traceback:")
        raise PrefilterError(f"Prefiltering failed: {e}")


def apply_business_rule_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function:
        - Apply business logic filters to remove obvious non-product pages.

        Business Rules:
            - Remove obvious person names (firstname_lastname patterns)
            - Remove meta/admin pages (Wikipedia:, Category:, etc.)
            - Remove historical retrospectives (history_of, timeline_of)
            - Remove pure year pages (2023, 2024, etc.)
            - Remove geographic patterns (City,_State)

        These rules reduce LLM workload by removing clear non-products
        before expensive AI processing.

    Args:
        df: DataFrame with page_title column

    Returns:
        Filtered DataFrame
    """
    initial_count = len(df)

    logger.info("Applying business rule filters...")

    # Business Rule 1: Remove meta/admin pages
    meta_patterns = [
        r"^wikipedia:",
        r"^category:",
        r"^template:",
        r"^portal:",
        r"^help:",
        r"^file:",
    ]

    # Business Rule 2: Remove historical content
    historical_patterns = [
        r"^list_of",
        r"^timeline_of",
        r"^history_of",
        r"^outline_of",
        r"^comparison_of",
    ]

    # Business Rule 3: Remove pure years
    year_patterns = [
        r"^\d{4}$",  # Just a year: 2023
        r"^\d{4}_in",  # 2023_in_technology
        r"^in_\d{4}",  # in_2023
    ]

    # Business Rule 4: Remove geographic locations
    geo_patterns = [
        r",_\w{2}$",  # City, State format (e.g., "Seattle,_WA")
        r",_[A-Z][a-z]+$",  # City, Country (e.g., "Paris,_France")
    ]

    # Business Rule 5: Remove obvious person name patterns
    person_patterns = [
        r"^\w+_\w+_\w+$",  # firstname_middlename_lastname
    ]

    # Combine all patterns
    all_patterns = (
        meta_patterns
        + historical_patterns
        + year_patterns
        + geo_patterns
        + person_patterns
    )

    combined_pattern = "|".join(all_patterns)

    # Apply filter
    df_filtered = df[
        ~df["page_title"].str.contains(
            combined_pattern, case=False, regex=True, na=False
        )
    ]

    removed = initial_count - len(df_filtered)
    removal_rate = (removed / initial_count * 100) if initial_count > 0 else 0

    logger.info(f"Business rules removed {removed:,} records ({removal_rate:.1f}%)")
    logger.info(f"Remaining: {len(df_filtered):,} records")

    return df_filtered


def get_warehouse_statistics(processing_date: str = "CURRENT_DATE") -> dict:
    """
    Function:
        - Get statistics about data available in warehouse for pre-filtering.
        - Useful for monitoring and debugging.

    Args:
        - processing_date: Date to check (default: 'CURRENT_DATE')

    Returns:
        - Dictionary with warehouse statistics
    """
    try:
        conn_string = (
            f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}"
            f"@core_sentiment_db:5432/{config.DB_NAME}"
        )
        engine = create_engine(conn_string)

        query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE count_views >= 100) as above_threshold,
                COUNT(*) FILTER (WHERE domain = 'en.wikipedia.org') as english_wiki,
                MIN(count_views) as min_views,
                MAX(count_views) as max_views,
                AVG(count_views) as avg_views
            FROM raw_pageviews
            WHERE processing_date = {processing_date}
        """

        result = pd.read_sql(query, engine)

        stats = {
            "total_records": int(result["total_records"].iloc[0]),
            "above_threshold": int(result["above_threshold"].iloc[0]),
            "english_wiki": int(result["english_wiki"].iloc[0]),
            "min_views": int(result["min_views"].iloc[0]),
            "max_views": int(result["max_views"].iloc[0]),
            "avg_views": float(result["avg_views"].iloc[0]),
        }

        logger.info("Warehouse Statistics:")
        logger.info(f"  Total records: {stats['total_records']:,}")
        logger.info(f"  Above threshold (100): {stats['above_threshold']:,}")
        logger.info(f"  English Wikipedia: {stats['english_wiki']:,}")
        logger.info(f"  View range: {stats['min_views']:,} - {stats['max_views']:,}")
        logger.info(f"  Average views: {stats['avg_views']:,.2f}")

        return stats

    except Exception as e:
        logger.error(f"Failed to get warehouse statistics: {e}")
        return {}


def validate_warehouse_data(processing_date: str = "CURRENT_DATE") -> bool:
    """
    Function:
        - Validate that warehouse has data for the specified date.

    Args:
        - processing_date: Date to validate

    Returns:
        - True if data exists, False otherwise
    """
    try:
        stats = get_warehouse_statistics(processing_date)

        if stats.get("total_records", 0) == 0:
            logger.error(f"No data in warehouse for date: {processing_date}")
            return False

        if stats.get("above_threshold", 0) == 0:
            logger.warning(f"No records above threshold for date: {processing_date}")
            return False

        logger.info(f"✓ Warehouse validation passed for date: {processing_date}")
        return True

    except Exception as e:
        logger.error(f"Warehouse validation failed: {e}")
        return False
