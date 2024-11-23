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
    @task 
    def create_table():
        # Initialize Postgreshook
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
        
        # SQL query to create the table 
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            media_type VARCHAR(50),
            date DATE,
        );
        """

        # Execute the SQL query
        pg_hook.run(create_table_query)

        return "Table created successfully"

    # Step 2: Extract the NASA API Data (APOD)-Astronomy Picture of the Day 

    # kc5L7Kq7fvPIMYONOsByxAS8u1dMgbomS9sB199u 
    # https://api.nasa.gov/planetary/apod?api_key=kc5L7Kq7fvPIMYONOsByxAS8u1dMgbomS9sB199u

    extract_apod = SimpleHttpOperator(
        task_id = "extract_apod",
        http_conn_id = "nasa_api",  # connection id defined in airflow for nasa api 
        endpoint = "/planetary/apod", # nasa api endpoint for APOD 
        method = "GET",
        data={"api_key" : "{{ conn.nasa_api.extra_dejson.api_key }}"}, # use the api key from the connection 
        response_filter = lambda response: response.json(), # filter the response to json
        log_response = True 
    )

    # Step 3: Transform the data (Pick the information that I need to save)

    # Step 4: Load the data into the PostgreSQL database

    # Step 5: Verify the data using DBeaver 

    # Step 6: Define the dependencies 