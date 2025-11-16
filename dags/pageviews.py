import logging
from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import TaskGroup, dag, task
from core_sentiment.include.app.tasks.cleanup import cleanup_temp_files

# Import business logic functions
from core_sentiment.include.app.tasks.download_data import download_random_wiki_file
from core_sentiment.include.app.tasks.extract_data import extract_data
from core_sentiment.include.app.tasks.llm_filter import process_with_llm
from core_sentiment.include.app.tasks.load_filtered_data import (
    load_filtered_pageviews_to_db,
)
from core_sentiment.include.app.tasks.load_raw_data import (
    load_raw_pageviews_to_db,
    verify_load,
)
from core_sentiment.include.app.tasks.prefilter_data import prefilter_from_db
from core_sentiment.include.app.utils.pageviews_filtering_prompt import SYSTEM_PROMPT
from pendulum import datetime

logger = logging.getLogger(__name__)


# @task()
# def load_data_to_database(extract_info: dict):
#     """Load extracted data into PostgreSQL."""
#     try:
#         logger.info("Starting load task…")

#         csv_path = extract_info["csv_path"]
#         source_file = extract_info["source_file"]

#         # Load data
#         rows_inserted = load_to_postgres(csv_path, source_file)

#         logger.info(f"Data loaded: {rows_inserted} rows")

#         return {
#             "rows_inserted": rows_inserted,
#             "source_file": source_file,
#             "status": "success",
#         }

#     except DatabaseError as e:
#         logger.error(f"Database load failed: {e}")
#         raise
#     except Exception as e:
#         logger.error(f"Unexpected error in load: {e}")
#         raise


default_args = {
    "owner": "dki",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="Core_Sentiment_Data_Pipeline",
    default_args=default_args,
    description="ETL pipeline to analyze Wikipedia pageviews for tech companies",
    schedule="@daily",
    start_date=datetime(2025, 10, 20),
    template_searchpath="/opt/airflow/dags/core_sentiment/include/sql/",
    max_active_runs=1,
    tags=[
        "wikipedia",
        "etl",
        "pageviews",
        "data pipeline",
        "core sentiment",
        "analytics",
    ],
    doc_md="""
    # Wikipedia Pageviews ETL Pipeline


    ## Purpose
    Extracts Wikipedia pageview data for major tech companies and analyzes trends.
    Follows proper ELT pattern: Extract → Load Raw → Transform → Load Filtered → Analyze

    ## Companies Tracked
    - Amazon
    - Apple
    - Facebook (Meta)
    - Google (Alphabet)
    - Microsoft

    ## Pipeline Steps
    1. **Initialize Database**: Create schema and tables
    2. **Download**: Fetch hourly Wikipedia pageview dump
    3. **Extract**: Unzip the gzip pageviews file and extract all data
    4. **Load Raw Data**: Store ALL raw pageviews in database (data warehouse)
    5. **Pre-filter**: Create subset for LLM processing
    6. **LLM Filter**: Use Ollama to filter genuine product/service pages
    7. **Load Filtered Data**: Store filtered results in database
    8. **Analyze**: Determine company with highest pageviews
    9. **Cleanup**: Remove temporary files

    ## Output
    Database Tables
    - `raw_pageviews`: All extracted pageviews (immutable, historical record)
    - `filtered_pageviews`: LLM-filtered product/service pages only
    - `company_pageview_summary`: Daily aggregated pageviews by company
    """,
)
def pageviews():
    # ========================================================
    # ============== DATABASE INITIALIZATION =================
    # ========================================================
    with TaskGroup("database_setup", tooltip="Initialize database schema") as db_setup:
        create_raw_tables = SQLExecuteQueryOperator(
            task_id="create_raw_tables",
            sql="ddl/tables.sql",
            conn_id="core_sentiment_db",
        )

        create_classifier_function = SQLExecuteQueryOperator(
            task_id="create_classifier_function",
            sql="ddl/company_classifier_function.sql",
            conn_id="core_sentiment_db",
        )

        create_overrides_table = SQLExecuteQueryOperator(
            task_id="create_overrides_table",
            sql="ddl/company_overrides_table.sql",
            conn_id="core_sentiment_db",
        )

        create_classified_view = SQLExecuteQueryOperator(
            task_id="create_classified_view",
            sql="ddl/classified_pageviews_view.sql",
            conn_id="core_sentiment_db",
        )

        # Schema creation order
        (
            create_raw_tables
            >> create_classifier_function
            >> create_overrides_table
            >> create_classified_view
        )  # type: ignore[unused-expression]

    # ================================================================================
    # ==================== EXTRACT & LOAD RAW TO WAREHOUSE PHASE =====================
    # ================================================================================

    with TaskGroup(
        "extract_load_raw", tooltip="Extract data and load to warehouse"
    ) as extract_load:

        @task
        def download_data():
            """Download Wikipedia pageview dump"""
            return download_random_wiki_file()

        @task
        def extract_all_data(zipped_file: str):
            """Extract all pageview data from archive"""
            return extract_data(zipped_file)

        @task
        def load_raw_to_warehouse(extract_info: dict):
            """Load ALL raw data to warehouse"""
            # Load data
            result = load_raw_pageviews_to_db(extract_info)

            # Verify data
            verification = verify_load(result["source_file"])

            # Combine results
            return {**result, "verification": verification}

        download_task = download_data()
        extract_task = extract_all_data(download_task)  # type: ignore[arg-type]
        load_raw_task = load_raw_to_warehouse(extract_task)  # type: ignore[arg-type]

        download_task >> extract_task >> load_raw_task  # type: ignore[unused-expression]

    # ========================================================
    # =================== TRANSFORM PHASE ====================
    # ========================================================

    with TaskGroup(
        "transform_llm", tooltip="Pre-filter and LLM filtering"
    ) as transform:

        @task
        def prefilter_data():
            """Pre-filter data from warehouse for LLM processing"""
            return prefilter_from_db()

        @task
        def llm_filter_products(csv_file: str):
            """
            LLM-powered filtering to identify genuine product/service pages.

            Business Logic:
                - Uses direct Ollama API calls (bypasses @task.llm decorator)
                - Processes data in batches for efficiency
                - Filters to keep only legitimate company products/services
                - Removes people, places, events, legal cases
            """
            return process_with_llm(
                prefiltered_csv_file=csv_file,
                system_prompt=SYSTEM_PROMPT,
                batch_size=50,
            )

        prefilter_task = prefilter_data()
        filter_task = llm_filter_products(prefilter_task)  # type: ignore[arg-type]

        prefilter_task >> filter_task  # type: ignore[unused-expression]

    # ========================================================
    # ============ LOAD FILTERED & ANALYTICS =================
    # ========================================================

    with TaskGroup(
        "load_analytics", tooltip="Load filtered data and run analytics"
    ) as analytics:

        @task
        def load_filtered_data(filtered_result: dict):
            """Load LLM-filtered data to curated layer"""
            return load_filtered_pageviews_to_db(filtered_result)

        load_filtered_task = load_filtered_data(filter_task)  # type: ignore[arg-type]

        # Refresh materialized view
        refresh_view = SQLExecuteQueryOperator(
            task_id="refresh_classified_view",
            sql="SELECT refresh_classified_pageviews();",
            conn_id="core_sentiment_db",
        )

        # Get company rankings
        get_rankings = SQLExecuteQueryOperator(
            task_id="get_company_rankings",
            sql="queries/company_rankings.sql",
            conn_id="core_sentiment_db",
            do_xcom_push=True,
        )

        # Get winner
        get_winner = SQLExecuteQueryOperator(
            task_id="get_biggest_company",
            sql="queries/biggest_company.sql",
            conn_id="core_sentiment_db",
            do_xcom_push=True,
        )

        load_filtered_task >> refresh_view >> [get_rankings, get_winner]  # type: ignore[unused-expression]

    # ========================================================
    # ====================== CLEAN UP ========================
    # ========================================================

    @task
    def cleanup_temp(csv_path: str):
        """Airflow Task: Remove temporary files after successful processing."""
        return cleanup_temp_files(csv_path)

    cleanup_task = cleanup_temp(prefilter_task)  # type: ignore[arg-type]

    # ========================================================
    # ================ PIPELINE FLOW (ETL) ===================
    # ========================================================

    db_setup >> extract_load >> transform >> analytics >> cleanup_task  # type: ignore[unused-expression]


pageviews()
