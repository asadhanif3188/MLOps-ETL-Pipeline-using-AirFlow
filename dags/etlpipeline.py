from airflow import DAG 
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

# Define DAG
with DAG(
    dag_id="nasa_apod_postgre",
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    # Step 1: Create the table if it doesn't exist 

    # Step 2: Extract the NASA API Data (APOD) 

    # Step 3: Transform the data (Pick the information that I need to save)

    # Step 4: Load the data into the PostgreSQL database

    # Step 5: Verify the data using DBeaver 

    # Step 6: Define the dependencies 