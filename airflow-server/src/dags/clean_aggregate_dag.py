from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pySpark.clean_tables import clean_data_tables
from pySpark.aggregate_tables import aggregate_data_tables


default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id="pre_process",
    default_args=default_args,
    start_date=datetime(2024, 12, 1),
    schedule_interval=None,
) as dag:
    
    clean_task = PythonOperator(
        task_id='clean_task',
        python_callable=clean_data_tables
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_task',
        python_callable=aggregate_data_tables
    )

    # The data must first be cleaned then aggregated
    clean_task >> aggregate_task