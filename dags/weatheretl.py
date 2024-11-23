from airflow import DAG 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.decorators import task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('OPEN_WEATHER_API_KEY')
CITY_NAME = os.getenv('CITY_NAME')
# --------------------------------------------

with DAG(
    dag_id = "weather_etl_pipeline",
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    # Task 1: Check if API is ready/active
    is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'open_weather_connection',
        endpoint = f"data/2.5/weather?q={CITY_NAME}&appid={API_KEY}"
    )

    # Task 2: Check if API is ready/active
    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'open_weather_connection',
        endpoint = f"data/2.5/weather?q={CITY_NAME}&appid={API_KEY}",
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )


    # Define the task dependencies
    is_weather_api_ready >> extract_weather_data


    