import logging
from datetime import timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from core_sentiment.include.configuration.settings import config
from core_sentiment.include.src_python.download_data import download_random_wiki_file
from core_sentiment.include.src_python.extract_data import extract_data
from core_sentiment.include.src_python.pageviews_filtering_prompt import SYSTEM_PROMPT
from core_sentiment.include.src_python.prefilter_data import prefilter_data
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
    template_searchpath="/opt/airflow/dags/core_sentiment/include/src_sql",
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
        """Airflow Task: Pre-filter pageview data for company-related pages."""
        return prefilter_data(csv_file)

    # Task 5: Filter data using LLM
    @task.llm(
        model="gpt-4o-mini",
        output_type=dict,
        max_active_tis_per_dagrun=1,
        system_prompt=SYSTEM_PROMPT,
    )
    def filter_pageview_data(prefiltered_csv_file):
        """
        Airflow Task:
            - LLM Task: Filter Wikipedia pageview data to keep only genuine product/service pages.
            - This receives a pre-filtered dataset to avoid memory issues.

        Argument:
            - prefiltered_csv_file (str): Path to the pre-filtered CSV file

        Return:
            dict: {
                "json_output": <list of dict>,
                "csv_output": <csv-formatted string>
            }
        """
        import pandas as pd

        try:
            # Read the CSV file
            logger.info(f"Reading pre-filtered CSV file: {prefiltered_csv_file}")
            df = pd.read_csv(prefiltered_csv_file)

            # Convert to list of dictionaries for LLM processing
            records = df.to_dict("records")
            logger.info(f"Loaded {len(records)} records to LLM for filtering")

            # The LLM will process these records using the SYSTEM_PROMPT logic
            # and return filtered results in both JSON and CSV formats
            return {
                "json_output": records,  # LLM will filter and return relevant records
                "csv_output": df.to_csv(
                    index=False
                ),  # LLM will filter and return as CSV
            }

        except Exception as e:
            logger.error(f"Error reading CSV file {prefiltered_csv_file}: {e}")
            raise

    # Task 6: Save filtered output
    @task
    def save_filtered_pageview_data(filtered_result):
        """
        Airflow Task:
            - Saves JSON and CSV outputs to local disk instead of storing large data in XCom.

        Argument:
            - filtered_result (dict): The result of the LLM task

        Return:
            - Metadata only.
        """

        import json

        try:
            # Ensure processed directory exists
            processed_dir = Path(config.PROCESSED_PAGEVIEWS_DIR)
            processed_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Processed directory: {processed_dir}")

            # Generate timestamp for unique filenames
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            json_path = processed_dir / f"filtered_{timestamp}.json"
            csv_path = processed_dir / f"filtered_{timestamp}.csv"

            # Validate filtered_result structure
            if not isinstance(filtered_result, dict):
                raise ValueError(f"Expected dict, got {type(filtered_result)}")

            json_output = filtered_result.get("json_output", [])
            csv_output = filtered_result.get("csv_output", "")

            # Write to JSON
            logger.info(f"Writing JSON output to: {json_path}")
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(json_output, json_file, indent=2, ensure_ascii=False)

            # Verify JSON file was created
            if not json_path.exists():
                raise IOError(f"Failed to create JSON file: {json_path}")
            logger.info(
                f"JSON file created successfully: {json_path.stat().st_size} bytes"
            )

            # Write to CSV
            logger.info(f"Writing CSV output to: {csv_path}")
            with open(csv_path, "w", encoding="utf-8") as csv_file:
                csv_file.write(csv_output)

            # Verify CSV file was created
            if not csv_path.exists():
                raise IOError(f"Failed to create CSV file: {csv_path}")
            logger.info(
                f"CSV file created successfully: {csv_path.stat().st_size} bytes"
            )

            result = {
                "json_file": str(json_path),
                "csv_file": str(csv_path),
                "json_records_count": len(json_output),
                "status": "success",
            }

            logger.info(f"Filtered output saved successfully: {result}")
            return result

        except (IOError, OSError) as e:
            logger.error(f"File I/O error while saving filtered output: {e}")
            raise

        except ValueError as e:
            logger.error(f"Invalid data format: {e}")
            raise

        except Exception as e:
            logger.error(f"Unexpected error while saving filtered output: {e}")
            raise

    # # Task 4: Load to Database
    # load_task = PythonOperator(
    #     task_id="load_to_database",
    #     python_callable=load_data_to_database,
    #     doc_md="Load extracted data into PostgreSQL database",
    # )

    # Define tasks
    download_task = download_pageview_data()
    extract_task = extract_pageview_data(download_task)
    prefilter_task = prefilter_pageview_data(extract_task)
    filter_task = filter_pageview_data(prefilter_task)
    save_task = save_filtered_pageview_data(filter_task)

    (
        create_pageviews_table_task
        >> download_task
        >> extract_task
        >> prefilter_task
        >> filter_task
        >> save_task
    )


pageviews()
