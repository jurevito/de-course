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

# Create Spark Submit task.
submit_job = SparkSubmitOperator(
	application ='/data/job.py',
	conn_id= 'spark_container',
	task_id='spark_submit_task',
    verbose=1,
	dag=dag
)

first_task >> submit_job