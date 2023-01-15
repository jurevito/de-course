# Apache Airflow.
from airflow import DAG, task
from airflow.contrib.sensors.file_sensor import FileSensor

# Apache Airflow operators.
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator

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

def generate_query(json_object):
    # , journal_ref: '%s', title: '%s', update_date: date('%s'), n_pages: %s, n_figures: %s
    query = "CREATE (n:Publication {doi: '%s'})"
    query = query % (
        json_object['doi'],
        #json_object['journal_ref'],
        #json_object['title'],
        #json_object['update_date'],
        #json_object['n_pages'],
        #json_object['n_figures'],
    )

    return query

def neo4j_queries(file_path):

    queries = []

    with open(file_path, 'r') as json_file:
        for line in json_file:
            json_object = json.loads(line)
            queries.append(generate_query(json_object))

    return ';'.join(queries)


default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG.
dag = DAG(
    dag_id='ingestion',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
)

#first_task = BashOperator(
#    task_id='first_task',
#    bash_command='echo "I will execute job on Spark node."',
#    dag=dag,
#)
#
## Fetches information about publication category
## and saves it into JSON file.
#categories_task = PythonOperator(
#    task_id='fetch_categories',
#    python_callable=get_categories,
#    dag=dag
#)
#
## Submits a Spark job which transforms data.
#submit_job = SparkSubmitOperator(
#    application ='/data/job.py',
#    conn_id= 'spark_container',
#    task_id='spark_submit',
#    name='airflow-spark',
#    verbose=1,
#    dag=dag
#)

# Executes a query on Neo4j database.
neo4j_task = Neo4jOperator(
    task_id="neo4j_query",
    neo4j_conn_id="neo4j_container",
    sql='%s' % neo4j_queries('/data/output/part-00000-fbdb61b5-4ed7-4ade-b33f-f779724dea6b-c000.json'),
    dag=dag,
)

# Setup order of execution.
#first_task >> submit_job >> neo4j_task
#first_task >> categories_task >> neo4j_task