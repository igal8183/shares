from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import requests


# General Configs #
dag_name = 'reali_6_example_dynamic_task'

# Dag #
default_args = { 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 0    
} 

dag = DAG(dag_name, 
description=dag_name,
start_date=datetime(2017, 3, 20), 
catchup=False, 
max_active_runs=1,
schedule_interval = None,
default_args=default_args)

tables = [
        'Loan'
        ,'RateLock'
        ,'Borrower'
        ,'Shipping'
        ]



# Functions #
def run_on_table(**kwargs):
    print(kwargs['table_name'])


# Tasks #
pipeline_start = DummyOperator(
    task_id='pipeline_start',
    dag=dag
)


pipeline_finish = DummyOperator(
    task_id='pipeline_finish',
    dag=dag
)


for table in tables:
    task = PythonOperator(
        task_id='etl_' + table,
        provide_context=True,
        python_callable=run_on_table,
        op_kwargs={'table_name': table},
        dag=dag,
    )
    pipeline_start >> task >> pipeline_finish
