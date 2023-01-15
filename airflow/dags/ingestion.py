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
from py2neo import Graph

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

def escape_chars(string, chars_to_escape):
    escaped_string = string
    for char in chars_to_escape:
        escaped_string = escaped_string.replace(char, '\\'+char)
    return escaped_string

def json_to_cypher_fields(json_object: dict, date_fields: list):

    fields = []

    for key, val in json_object.items():
        if val is not None:
            if key in date_fields:
                fields.append(f"{key}: date('{val}')")
            elif isinstance(val, str):
                tmp = escape_chars(val, ['\''])
                fields.append(f"{key}: '{tmp}'")
            else:
                fields.append(f"{key}: {val}")

    fields_str = ','.join(fields)
    return '{%s}' % fields_str

def neo4j_queries():
    file_path = '/data/output/part-00000-32717f90-3564-4323-bc06-a1e708d22acf-c000.json'
    graph = Graph('bolt://neo4j:7687', auth=('neo4j', 'admin'), name='neo4j')

    with open(file_path, 'r') as json_file:
        for line in json_file:
            json_object: dict = json.loads(line)


            publication = {
                'doi': json_object.get('doi', None),
                'journal_ref': json_object.get('journal_ref', None),
                'report_number': json_object.get('report_number', None),
                'title': json_object.get('title', None),
                'update_date': json_object.get('update_date', None),
                'n_pages': json_object.get('n_pages', None),
                'n_figures': json_object.get('n_figures', None),
            }

            fields = json_to_cypher_fields(publication, ['update_date'])
            query = """
            MERGE (n:Publication {doi: '%s'})
            SET n = %s
            """ % (publication['doi'], fields)
            graph.run(query)

            authors = []
            for author in json_object['authors']:
                if author['name'] is not None:
                    person = {
                        'name': author['name'],
                        'last_name': author['last_name'],
                    }

                authors.append(json_to_cypher_fields(person, []))
            
            fields = '[%s]' % ','.join(authors)
            query = """
            MATCH (n:Publication {doi: '%s'})
            UNWIND %s AS person
            MERGE (p:Person {name: person.name, last_name: person.last_name}) SET p = person
            MERGE (p)-[:IS_AUTHOR]->(n)
            """ % (publication['doi'], fields)
            graph.run(query)
            

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
neo4j_task = PythonOperator(
    task_id='create_nodes',
    python_callable=neo4j_queries,
    dag=dag
)

# Setup order of execution.
#first_task >> submit_job >> neo4j_task
#first_task >> categories_task >> neo4j_task