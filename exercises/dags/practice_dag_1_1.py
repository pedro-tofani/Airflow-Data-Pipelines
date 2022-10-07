import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def greet():
  logging.info("Hello World")


dag = DAG(
        'practice_dag_1_1',
        start_date=datetime.datetime.now())


greet_task = PythonOperator(
   task_id="greet_task",
   python_callable=greet,
   dag=dag
)

