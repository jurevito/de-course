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
import os

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

def escape_chars(string, chars_to_escape=['\'']):
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
                tmp = escape_chars(val)
                fields.append(f"{key}: '{tmp}'")
            else:
                fields.append(f"{key}: {val}")

    fields_str = ','.join(fields)
    return '{%s}' % fields_str

def neo4j_queries():
    file_path = '/data/output/processed.json'
    graph = Graph('bolt://neo4j:7687', auth=('neo4j', 'admin'), name='neo4j')

    with open('/data/categories.json', 'r') as categories_file:
        category_objects = json.load(categories_file)
        categories = [json_to_cypher_fields(category, []) for category in category_objects]
        fields = '[%s]' % ','.join(categories)

        query = """
        UNWIND %s AS category
        MERGE (c:Category {code: category.code}) SET c = category
        """ % (fields)
        graph.run(query)


    with open(file_path, 'r') as json_file:
        for line in json_file:
            json_object: dict = json.loads(line)


            publication = {
                'doi': json_object.get('doi', None),
                'report_number': json_object.get('report_number', None),
                'title': json_object.get('title', None),
                'update_date': json_object.get('update_date', None),
                'n_pages': json_object.get('n_pages', None),
                'n_figures': json_object.get('n_figures', None),
            }

            codes = ["'%s'" % code for code in json_object['categories']]
            code_strs = "[%s]" % (','.join(codes))

            # Add "Publication" nodes.
            fields = json_to_cypher_fields(publication, ['update_date'])
            query = """
            MATCH (c:Category)
            WHERE c.code IN %s
            MERGE (n:Publication {doi: '%s'})
            SET n = %s
            MERGE (n)-[:IS_IN]->(c)
            """ % (code_strs, publication['doi'], fields)
            graph.run(query)

            authors = []
            for author in json_object['authors']:
                if author['name'] is not None:
                    person = {
                        'name': author['name'],
                        'last_name': author['last_name'],
                    }

                authors.append(json_to_cypher_fields(person, []))
            
            # Add "Person" nodes and "IS_AUTHOR" relationships.
            fields = '[%s]' % ','.join(authors)
            query = """
            MATCH (n:Publication {doi: '%s'})
            UNWIND %s AS person
            MERGE (p:Person {name: person.name, last_name: person.last_name}) SET p = person
            MERGE (p)-[:IS_AUTHOR]->(n)
            """ % (publication['doi'], fields)
            graph.run(query)
            
            # Add "SUBMITTED" relationships.
            if json_object['submitter']['name'] is not None:
                query = """
                MATCH (n:Publication {doi: '%s'}), (p:Person {name: '%s', last_name: '%s'})
                MERGE (p)-[:SUBMITTED]->(n)
                """ % (publication['doi'], escape_chars(json_object['submitter']['name']), escape_chars(json_object['submitter']['last_name']))

                graph.run(query)

def rename_file(**kwargs):
    path = kwargs['path']
    new_name = kwargs['new_name']

    file_name = None
    for file in os.listdir(path):
        if file.endswith('.json'):
            file_name = file
            break
    
    assert file_name is not None

    file_path = os.path.join(path, file_name)
    new_file_path = os.path.join(path, new_name)
    os.rename(file_path, new_file_path)

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG.
dag = DAG(
    dag_id='ingestion',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
)

# Fetches information about publication category
# and saves it into JSON file.
categories_task = PythonOperator(
    task_id='fetch_categories',
    python_callable=get_categories,
    dag=dag
)

# Submits a Spark job which transforms data.
submit_job = SparkSubmitOperator(
    application ='/data/job.py',
    conn_id= 'spark_container',
    task_id='spark_submit',
    name='airflow-spark',
    verbose=1,
    dag=dag
)

# Waits for a data to appear in staging area.
# It is then consumed by the pipeline.
file_sensor = FileSensor(
    task_id='file_sensor',
    filepath='/data/staging',
    fs_conn_id='fs_default',
    poke_interval=5,
    dag=dag
)

# Renames a raw file before Spark job reads it.
rename_raw_task = PythonOperator(
    task_id='rename_raw_file',
    python_callable=rename_file,
    op_kwargs={'path': '/data/staging', 'new_name': 'raw.json'},
    dag=dag
)

# Deletes the raw data file.
delete_raw_task = BashOperator(
    task_id='delete_raw_file',
    bash_command='rm /data/staging/raw.json',
    dag=dag,
)

# Renames processed data file before
# Neo4J reads it.
rename_processed_task = PythonOperator(
    task_id='rename_processed_file',
    python_callable=rename_file,
    op_kwargs={'path': '/data/output', 'new_name': 'processed.json'},
    dag=dag
)

# Executes a query on Neo4j database.
neo4j_task = PythonOperator(
    task_id='neo4j_task',
    python_callable=neo4j_queries,
    dag=dag
)

# Delete the processed data file after
# it inserted into database.
delete_processed_task = BashOperator(
    task_id='delete_processed_file',
    bash_command='rm -r /data/output',
    dag=dag,
)

# Setup order of execution.
file_sensor >> rename_raw_task >> submit_job >> delete_raw_task >> rename_processed_task >> neo4j_task >> delete_processed_task
categories_task >> neo4j_task
