from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from co2_data_etl import fetch_co2_data 

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    'co2_data_etl',
    default_args=default_args,
    description='ETL for fetching and storing CO₂ emissions data',
    schedule_interval=None,
    catchup=False,
)

# Define the task to execute `fetch_co2_data`
fetch_co2_data_task = PythonOperator(
    task_id='fetch_co2_data',
    python_callable=fetch_co2_data,
    dag=dag,
)

# Set task dependencies (if needed, e.g., multiple tasks)
fetch_co2_data_task
