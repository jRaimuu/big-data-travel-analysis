
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from climate_api_etl import fetch_weather_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['liamsarj4@gmail.com'],
    'email_on_failure': True,
}

dag = DAG(
    'climate_data_etl',
    default_args=default_args,
    description='ETL for fetching and storing weather data',
    schedule_interval=timedelta(minutes=5), 
    catchup=False,
)

# task that executes fetch_weather_data function
fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

fetch_weather_data_task