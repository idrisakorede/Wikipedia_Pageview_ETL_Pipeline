# load_to_db.py
"""Database loading utilities for Wikipedia pageview data."""

import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

from dags.utils.configs import DB_CONFIG
from logger import get_logger

logger = get_logger(__name__)


class DatabaseError(Exception):
    """Custom exception for database errors."""

    pass


def get_db_connection():
    """
    Create and return a database connection.

    Returns:
        psycopg2 connection object

    Raises:
        DatabaseError: If connection fails
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise DatabaseError(f"Database connection failed: {e}")


def init_database():
    """
    Initialize database schema with proper constraints.
    Creates tables if they don't exist.

    Raises:
        DatabaseError: If initialization fails
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Create main table with proper constraints
        cur.execute("""
            CREATE TABLE IF NOT EXISTS company_pageviews (
                id SERIAL PRIMARY KEY,
                company_name TEXT NOT NULL,
                pageviews INTEGER NOT NULL CHECK (pageviews >= 0),
                source_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_company_file UNIQUE (company_name, source_file)
            );
        """)

        # Create index for faster queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_company_pageviews_created 
            ON company_pageviews(created_at DESC);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_company_pageviews_company 
            ON company_pageviews(company_name);
        """)

        # Create summary view for easy analysis
        cur.execute("""
            CREATE OR REPLACE VIEW company_pageviews_summary AS
            SELECT 
                company_name,
                SUM(pageviews) as total_pageviews,
                COUNT(*) as load_count,
                MAX(created_at) as last_updated
            FROM company_pageviews
            GROUP BY company_name
            ORDER BY total_pageviews DESC;
        """)

        conn.commit()
        cur.close()
        conn.close()

        logger.success("Database schema initialized successfully")

    except psycopg2.Error as e:
        logger.error(f"Failed to initialize database: {e}")
        raise DatabaseError(f"Schema creation failed: {e}")


def load_to_postgres(csv_path: str, source_file: Optional[str] = None) -> int:
    """
    Load company pageviews data into PostgreSQL with idempotence.

    Args:
        csv_path: Path to the CSV file containing extracted data
        source_file: Original source file name for tracking (optional)

    Returns:
        Number of rows inserted

    Raises:
        DatabaseError: If loading fails
    """
    if not Path(csv_path).exists():
        raise DatabaseError(f"CSV file not found: {csv_path}")

    if source_file is None:
        source_file = Path(csv_path).stem

    logger.info(f"Loading data from: {csv_path}")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Read CSV data
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            data = list(reader)

        if not data:
            logger.warning("No data to load from CSV")
            return 0

        # Prepare data for insertion
        values = [
            (row["company_name"], int(row["count_views"]), source_file, datetime.now())
            for row in data
        ]

        # Use ON CONFLICT to handle duplicates (idempotence)
        insert_query = """
            INSERT INTO company_pageviews (company_name, pageviews, source_file, created_at)
            VALUES %s
            ON CONFLICT (company_name, source_file) 
            DO UPDATE SET 
                pageviews = EXCLUDED.pageviews,
                created_at = EXCLUDED.created_at
            RETURNING id;
        """

        # Execute batch insert
        inserted = execute_values(
            cur, insert_query, values, template="(%s, %s, %s, %s)", fetch=True
        )

        rows_affected = len(inserted)
        conn.commit()

        cur.close()
        conn.close()

        logger.success(f"Successfully loaded {rows_affected} rows into database")
        return rows_affected

    except psycopg2.Error as e:
        logger.error(f"Database error during load: {e}")
        if "conn" in locals():
            conn.rollback()
        raise DatabaseError(f"Failed to load data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during load: {e}")
        raise DatabaseError(f"Load failed: {e}")


def get_top_company(limit: int = 1) -> list:
    """
    Query database for companies with highest pageviews.

    Args:
        limit: Number of top companies to return

    Returns:
        List of tuples (company_name, total_pageviews)

    Raises:
        DatabaseError: If query fails
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT company_name, total_pageviews
            FROM company_pageviews_summary
            ORDER BY total_pageviews DESC
            LIMIT %s;
        """,
            (limit,),
        )

        results = cur.fetchall()

        cur.close()
        conn.close()

        if results:
            logger.info(f"Top {limit} companies retrieved")
            for rank, (company, views) in enumerate(results, 1):
                logger.info(f"  #{rank}: {company} - {views:,} views")

        return results

    except psycopg2.Error as e:
        logger.error(f"Query failed: {e}")
        raise DatabaseError(f"Failed to query database: {e}")


def cleanup_old_data(days: int = 30) -> int:
    """
    Remove data older than specified days.

    Args:
        days: Number of days to keep

    Returns:
        Number of rows deleted
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            DELETE FROM company_pageviews
            WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
            RETURNING id;
        """,
            (days,),
        )

        deleted = cur.rowcount
        conn.commit()

        cur.close()
        conn.close()

        logger.info(f"Cleaned up {deleted} old records (older than {days} days)")
        return deleted

    except psycopg2.Error as e:
        logger.error(f"Cleanup failed: {e}")
        raise DatabaseError(f"Failed to cleanup data: {e}")
