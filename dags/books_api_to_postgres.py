# Importing necessary modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import os
import psycopg2


# Function to fetch book data from API

def get_book_data_from_API(ti):
    api_url = 'https://openlibrary.org/subjects/science.json'
    response = requests.get(api_url)
    book_data = response.json()  
    ti.xcom_push(key='book_data', value=book_data)
    return book_data

# Function to transform the data
def transform_the_data(ti):
    transformed_data = []
    book_data = ti.xcom_pull(key='book_data', task_ids='get_book_data_from_api')

    print("Pulled XCOM data: ", book_data)

    for book in book_data['works']:
        title = book['title']
        author = book['authors'][0]['name'] if 'authors' in book and book['authors'] else None
        publish_date = book['first_publish_year'] if 'first_publish_year' in book else None

        transformed_data.append((title, author, publish_date))
        ti.xcom_push(key='transformed_data', value=transformed_data)
    return transformed_data

# Function to insert records into PostgreSQL
def insert_records(ti):
    postgres_host = os.environ.get('postgres_host', 'host.docker.internal')
    postgres_database = os.environ.get('postgres_database', 'superset')
    postgres_user = os.environ.get('postgres_user', 'superset')
    postgres_password = os.environ.get('postgres_password', 'superset')
    postgres_port = os.environ.get('postgres_port', 5433)

    connection = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cursor = connection.cursor()

    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_the_data')
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS books_table (title VARCHAR, author VARCHAR, publish_date INT)"
    )

    for record in transformed_data:
        cursor.execute(
            "INSERT INTO books_table (title, author, publish_date) VALUES (%s, %s, %s)",
            record
        )
    connection.commit()
    cursor.close()
    connection.close()


# Defining the DAG
default_args = {
    'owner': 'efecaliskan',
    'start_date': datetime(2023, 10, 25),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('insert_books_into_postgres', 
    default_args=default_args, 
    schedule_interval='@daily', 
    catchup=False) as dag:

    get_book_data_from_api_task = PythonOperator(
        task_id='get_book_data_from_api',
        python_callable=get_book_data_from_API
    )
    
    transform_the_data_task = PythonOperator(
        task_id='transform_the_data',
        python_callable=transform_the_data
    )

    insert_records_task = PythonOperator(
        task_id='insert_records',
        python_callable=insert_records
    )

    get_book_data_from_api_task >> transform_the_data_task >> insert_records_task