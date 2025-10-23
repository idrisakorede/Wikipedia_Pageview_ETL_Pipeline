# dags/wikipedia_pageviews_dag.py
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import datetime
from utils.download_data import download_file, get_random_wiki_gz_link
from utils.extract_data import extract_company_data
from utils.load_to_db import load_to_postgres
from utils.logger import get_logger

logger = get_logger(__name__)

BASE_DIR = Path("/opt/airflow/data")
MONTH_URL = "https://dumps.wikimedia.org/other/pageviews/2025/2025-10/"


def _download(**context):
    gz_url = get_random_wiki_gz_link(MONTH_URL)
    file_name = gz_url.split("/")[-1]
    destination = BASE_DIR / file_name
    downloaded_file = download_file(gz_url, str(destination))
    context["ti"].xcom_push(key="zipped_file", value=downloaded_file)


def _extract(**context):
    zipped_file = context["ti"].xcom_pull(key="zipped_file")
    result_df, output_file = extract_company_data(zipped_file)
    context["ti"].xcom_push(key="output_csv", value=output_file)


def _load(**context):
    csv_file = context["ti"].xcom_pull(key="output_csv")
    load_to_postgres(csv_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 20),
    "retries": 1,
}

with DAG(
    dag_id="wikipedia_pageviews_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="ETL pipeline to process Wikipedia pageviews for tech companies.",
) as dag:
    download_task = PythonOperator(
        task_id="download_pageviews",
        python_callable=_download,
    )

    extract_task = PythonOperator(
        task_id="extract_company_data",
        python_callable=_extract,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=_load,
    )

    download_task >> extract_task >> load_task
