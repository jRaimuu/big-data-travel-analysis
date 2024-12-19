from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
import os
import subprocess

load_dotenv()

# constants
SUB_URI = os.getenv("SUB_URI")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT")
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
CLUSTER_NAME = "travel-spark-cluster"
REGION = "us-central1"


DISEASE_FETCH = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/jobs/fetch_apis/fetch_disease_data.py",
        "python_file_uris": [
            f"gs://{BUCKET_NAME}/scripts/dependencies/bucket_to_spark.env"
        ], 
    },
}

INFRASTRUCTURE_FETCH = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/jobs/fetch_apis/fetch_infrastructure_data.py",
        "python_file_uris": [
            f"gs://{BUCKET_NAME}/scripts/dependencies/bucket_to_spark.env"
        ], 
    },
}

OUR_WORLD_FETCH = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/jobs/fetch_apis/fetch_our_world_data.py",
        "python_file_uris": [
            f"gs://{BUCKET_NAME}/scripts/dependencies/bucket_to_spark.env"
        ], 
    },
}

# DAG definition
with DAG(
    'dataproc_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # fetch data from APIs
    fetch_disease_job = DataprocSubmitJobOperator(
        task_id="fetch_disease",
        job=DISEASE_FETCH,
        region=REGION,
        project_id=PROJECT_ID
    )

    fetch_infrastructure_job = DataprocSubmitJobOperator(
        task_id="fetch_infrastructure",
        job=INFRASTRUCTURE_FETCH,
        region=REGION,
        project_id=PROJECT_ID
    )

    fetch_our_world_job = DataprocSubmitJobOperator(
        task_id="fetch_our_world",
        job=OUR_WORLD_FETCH,
        region=REGION,
        project_id=PROJECT_ID
    )

    [fetch_disease_job, fetch_infrastructure_job, fetch_our_world_job]
