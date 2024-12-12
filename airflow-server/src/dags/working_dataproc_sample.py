from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
import os
load_dotenv()

# constants
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
CLUSTER_NAME = "travel-spark-cluster"
REGION = "us-central1"

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="us-central1-a",
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    worker_disk_size=30,
    master_disk_size=30,
    gce_cluster_config={
        "subnetwork_uri": "projects/phonic-sunbeam-443308-r6/regions/us-central1/subnetworks/default",       
        "internal_ip_only": True,
        "service_account": "1017993515337-compute@developer.gserviceaccount.com",
    },
   staging_bucket=BUCKET_NAME,
).make()

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}


# DAG definition
with DAG(
    'dataproc_spark_job',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    # delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",  # ensures cluster deletion runs even if tasks fail
    )

    # task dependencies
    create_cluster >> delete_cluster