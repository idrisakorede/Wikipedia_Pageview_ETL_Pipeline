import logging
from pathlib import Path

import pandas as pd
import psycopg
from core_sentiment.include.app_config.settings import config
from pendulum import now

logger = logging.getLogger(__name__)


class PrefilterError(Exception):
    """Raised when prefiltering from warehouse fails."""

    pass


def classify_company(page_title: str) -> str:
    """
    Function:
        Classify page title to tech company.

    Returns:
        Company name (Amazon, Apple, Google, Microsoft, Meta) or 'Other'
    """
    title_lower = page_title.lower()

    # Amazon
    if any(
        k in title_lower
        for k in ["amazon", "aws", "alexa", "kindle", "prime_video", "fire_tv", "echo_"]
    ):
        return "Amazon"

    # Apple
    elif any(
        k in title_lower
        for k in [
            "apple",
            "iphone",
            "ipad",
            "macbook",
            "imac",
            "ios",
            "macos",
            "airpods",
            "apple_watch",
            "app_store",
            "itunes",
        ]
    ):
        return "Apple"

    # Google
    elif any(
        k in title_lower
        for k in [
            "google",
            "android",
            "chrome",
            "youtube",
            "gmail",
            "pixel",
            "google_maps",
            "google_drive",
            "google_cloud",
            "nest_",
        ]
    ):
        return "Google"

    # Microsoft
    elif any(
        k in title_lower
        for k in [
            "microsoft",
            "windows",
            "xbox",
            "azure",
            "office_365",
            "teams",
            "outlook",
            "surface",
            "bing",
        ]
    ):
        return "Microsoft"

    # Meta
    elif any(
        k in title_lower
        for k in [
            "facebook",
            "meta",
            "instagram",
            "whatsapp",
            "oculus",
            "messenger",
            "threads",
        ]
    ):
        return "Meta"

    else:
        return "Other"


def prefilter_from_db(min_views: int = 1000) -> str:
    """
    Function:
        Runs the 3-stage SQL pipeline:
            Stage A → traffic filter
            Stage B → noise removal
            Stage C → tech-company keyword filter
        Adds company classification column
        Then exports to CSV for LLM processing.

    Args:
        min_views: Minimum view count threshold

    Return:
        CSV path containing the LLM-ready filtered rows.
    """

    logger.info("=" * 70)
    logger.info("RUNNING PREFILTER PIPELINE (A → B → C)")
    logger.info(f"Minimum views: {min_views:,}")
    logger.info("=" * 70)

    # Build DB connection string
    conn_string = (
        f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}"
        f"@core_sentiment_db:5432/{config.DB_NAME}"
    )

    # ---------- SQL PIPELINE ----------
    sql_query = f"""
        WITH
        stageA AS (
            SELECT domain, page_title, count_views
            FROM raw_pageviews
            WHERE processing_date = CURRENT_DATE
              AND count_views >= {min_views}
        ),

        stageB AS (
            SELECT *
            FROM stageA
            WHERE
                page_title NOT LIKE 'List_of%%'
                AND page_title NOT LIKE 'History_of%%'
                AND page_title NOT LIKE 'Timeline_of%%'
                AND page_title NOT LIKE 'Comparison_of%%'
                AND page_title NOT LIKE 'Outline_of%%'

                AND page_title NOT LIKE '%%(film)'
                AND page_title NOT LIKE '%%(movie)'
                AND page_title NOT LIKE '%%(book)'
                AND page_title NOT LIKE '%%(novel)'
                AND page_title NOT LIKE '%%(documentary)'

                AND page_title NOT LIKE 'Wikipedia:%%'
                AND page_title NOT LIKE 'Category:%%'
                AND page_title NOT LIKE 'Template:%%'
                AND page_title NOT LIKE 'Portal:%%'

                AND page_title NOT LIKE '%%_(person)'
                AND page_title NOT LIKE '%%born_%%'
                AND page_title NOT LIKE '%%_(actor)'
                AND page_title NOT LIKE '%%_(musician)'
                AND page_title NOT LIKE '%%_(entrepreneur)'
        ),

        stageC AS (
            SELECT *
            FROM stageB
            WHERE
                page_title ILIKE '%%Amazon%%'
                OR page_title ILIKE '%%Apple%%'
                OR page_title ILIKE '%%Google%%'
                OR page_title ILIKE '%%Alphabet%%'
                OR page_title ILIKE '%%Microsoft%%'
                OR page_title ILIKE '%%Meta%%'
                OR page_title ILIKE '%%Facebook%%'
                OR page_title ILIKE '%%Instagram%%'
                OR page_title ILIKE '%%WhatsApp%%'
                OR page_title ILIKE '%%AWS%%'
                OR page_title ILIKE '%%Android%%'
                OR page_title ILIKE '%%iPhone%%'
                OR page_title ILIKE '%%Windows%%'
                OR page_title ILIKE '%%Xbox%%'
                OR page_title ILIKE '%%macOS%%'
                OR page_title ILIKE '%%Azure%%'
                OR page_title ILIKE '%%iPad%%'
                OR page_title ILIKE '%%Oculus%%'
                OR page_title ILIKE '%%Pixel%%'
                OR page_title ILIKE '%%LinkedIn%%'
        )

        SELECT *
        FROM stageC
        ORDER BY count_views DESC;
    """

    try:
        logger.info("Executing SQL pipeline...")

        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)  # type: ignore[arg-type]
                rows = cur.fetchall()

                if cur.description is None:
                    raise PrefilterError("Query returned no column metadata")

                if not rows:
                    raise PrefilterError("Pipeline returned zero rows")

                columns = [col[0] for col in cur.description]
                df = pd.DataFrame(rows, columns=columns)

        logger.info(f"Pipeline output: {len(df):,} rows")

        # ---------- ADD COMPANY CLASSIFICATION ----------
        logger.info("Classifying companies...")
        df["company"] = df["page_title"].apply(classify_company)

        # Log classification distribution
        company_counts = df["company"].value_counts()
        logger.info("Company distribution:")
        for company, count in company_counts.items():
            logger.info(f"  {company}: {count:,} pages")

        # ---------- SAVE TO TEMP CSV ----------
        temp_dir = Path(config.PROCESSED_PAGEVIEWS_DIR)
        temp_dir.mkdir(parents=True, exist_ok=True)

        timestamp = now().format("YYYYMMDD_HHmmss")
        csv_path = temp_dir / f"prefiltered_{timestamp}.csv"

        df.to_csv(csv_path, index=False)

        logger.info(f"✓ Saved CSV for LLM: {csv_path}")
        logger.info("=" * 70)

        return str(csv_path)

    except Exception as e:
        logger.error(f"Prefilter pipeline failed: {e}")
        raise PrefilterError(str(e))
