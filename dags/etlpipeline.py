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
    @task
    def transform_apod_data(response):
        # Transform the data
        transformed_data = {
            "title": response.get("title", ""),
            "explanation": response.get("explanation", ""),
            "url": response.get("url", ""),
            "media_type": response.get("media_type", ""),
            "date": response.get("date", "")
        }

        return transformed_data
    
    # Step 4: Load the data into the PostgreSQL database
    @task
    def load_data_to_postgres(transformed_data):
        # Initialize Postgreshook
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')

        # SQL query to insert data into the table
        insert_query = """
        INSERT INTO nasa_apod_data (title, explanation, url, media_type, date)
        VALUES (%s, %s, %s, %s, %s);
        """

        # Execute the SQL query with the transformed data
        pg_hook.run(
            insert_query, 
            parameters = (
                transformed_data['title'],
                transformed_data['explanation'],
                transformed_data['url'],
                transformed_data['media_type'],
                transformed_data['date'],
                )
            )

        return "Data loaded successfully"

    # Step 5: Verify the data using DBeaver 

    # Step 6: Define the dependencies 