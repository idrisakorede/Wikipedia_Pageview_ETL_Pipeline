import logging
from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from core_sentiment.include.app.tasks.download_data import download_random_wiki_file
from core_sentiment.include.app.tasks.extract_data import extract_data
from core_sentiment.include.app.tasks.file_operations import save_filtered_output
from core_sentiment.include.app.tasks.llm_filtering import (
    call_ollama_api,
    process_batches,
)
from core_sentiment.include.app.tasks.prefilter_data import prefilter_data
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
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="Core_Sentiment_Data_Pipeline",
    default_args=default_args,
    description="ETL pipeline to analyze Wikipedia pageviews for tech companies",
    schedule="@daily",
    start_date=datetime(2025, 10, 20),
    catchup=False,
    template_searchpath="/opt/airflow/dags/core_sentiment/include/sql/ddl",
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

    ## Companies Tracked
    - Amazon
    - Apple
    - Facebook (Meta)
    - Google
    - Microsoft

    ## Pipeline Steps
    1. **Initialize Database**: Create schema if needed
    2. **Download**: Fetch hourly Wikipedia pageview dump
    3. **Extract**: Unzip the gzip pageviews file and extract data into a json and csv files
    4. **Filter**: Filter company data
    4. **Load**: Store data in PostgreSQL
    5. **Analyze**: Determine company with highest views
    6. **Cleanup**: Remove temporary files

    ## Output
    Database table `company_pageviews` with aggregated view counts.
    """,
)
def pageviews():
    # Task 1: Initialize Database (Creates table if not exists)
    create_pageviews_table_task = SQLExecuteQueryOperator(
        task_id="create_pageviews_table",
        sql="create_pageviews_table.sql",
        conn_id="core_sentiment_db",
        queue="default",
        doc_md="Create database schema and tables if they don't exist",
    )

    # Task 2: Download Data
    @task
    def download_pageview_data():
        """Airflow Task: Download Wikipedia pageview data file."""

        return download_random_wiki_file()

    # Task 3: Unzip and Extract Company Data
    @task
    def extract_pageview_data(zipped_file):
        """Airflow Task: Extract company-specific pageview data."""

        return extract_data(zipped_file)

    # Task 4: Pre-filter before LLM processing
    @task
    def prefilter_pageview_data(csv_file):
        """Airflow Task: Pre-filter pageview data."""
        return prefilter_data(csv_file)

    # Task 5: LLM Filter (Sequential Batches)
    @task
    def filter_pageview_data(prefiltered_csv_file):
        """Process data through LLM in sequential batches."""

        # Create wrapper function that includes system prompt
        def batch_processor(batch_records):
            return call_ollama_api(batch_records, SYSTEM_PROMPT)

        return process_batches(
            prefiltered_csv_file=prefiltered_csv_file,
            batch_processor_func=batch_processor,
            batch_size=50,
        )

    # Task 6: Save
    @task
    def save_filtered_data(filtered_result):
        """Airflow Task: Save filtered data to disk."""
        return save_filtered_output(filtered_result)

    # # Task 4: Load to Database
    # load_task = PythonOperator(
    #     task_id="load_to_database",
    #     python_callable=load_data_to_database,
    #     doc_md="Load extracted data into PostgreSQL database",
    # )

    # Define workflow
    download_task = download_pageview_data()
    extract_task = extract_pageview_data(download_task)
    prefilter_task = prefilter_pageview_data(extract_task)
    filter_task = filter_pageview_data(prefilter_task)
    save_task = save_filtered_data(filter_task)

    # Set dependencies
    create_pageviews_table_task
    download_task >> extract_task >> prefilter_task >> filter_task >> save_task


pageviews()
