# Apache Airflow.
from airflow import DAG, task
from airflow.contrib.sensors.file_sensor import FileSensor

# Apache Airflow operators.
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Other imports.
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import json

def get_categories():

    # Fetch HTML for categories.
    url = 'https://arxiv.org/category_taxonomy'
    response = requests.get(url)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')
    h4_elements = soup.find_all('h4')

    data = []

    # Extract code with name from each tag.
    for h4 in h4_elements:
        if h4.span is not None:
            
            name = h4.span.text[1:-1]
            h4.span.extract()
            code = h4.text.strip()

            data.append({
                'code': code,
                'name': name,
            })

    # Save it into a JSON file.
    with open('/data/categories.json', 'w+') as json_file:
        json.dump(data, json_file)


default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG.
dag = DAG(
    dag_id='spark',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
)

first_task = BashOperator(
    task_id='first_task',
    bash_command='echo "I will execute job on Spark node."',
    dag=dag,
)

# Fetches information about publication category
# and saves it into JSON file.
categories_task = PythonOperator(
    task_id='categories_task',
    python_callable=get_categories,
    dag=dag
)

# Sumbits a Spark job which transforms data.
submit_job = SparkSubmitOperator(
    application ='/data/job.py',
    conn_id= 'spark_container',
    task_id='spark_submit_task',
    name='airflow-spark',
    verbose=1,
    dag=dag
)

first_task >> submit_job
first_task >> categories_task