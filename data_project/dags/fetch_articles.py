from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
from config import API_KEY 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['mitchomzz@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catch_up': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    schedule_interval=timedelta(days=1), 
    default_args=default_args, 
    catchup=False, 
    tags=['news_articles']
)



def fetch_articles_workflow():
    
    @task()
    def extract():
        from extract_json import fetch_articles
        return fetch_articles(API_KEY)

    @task()
    def process_and_save(data):
        from blob_storage import store_articles
        store_articles(data)

    @task()
    def transform(data):
        from transformation import transform_data
        return transform_data(data)

    @task()
    def store_in_mysql(data):
        from load_to_mysql import load_data
        load_data(data)

    # Task flow
    extracted_data = extract()
    processed_data = process_and_save(extracted_data)
    transformed_data = transform(processed_data)
    load_data(transformed_data)


# Instantiate the DAG
fetch_articles_workflow()
