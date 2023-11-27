
from datetime import timedelta
from textwrap import dedent
from datetime import datetime, timedelta

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# where our file with dates are going to be created
now = datetime.now().strftime("%d-%m-%Y_%H_%M_%S")
filepath = '/home/german/dates/my_dag'+now+'.txt'

default_args = {
    'owner': 'german Mendez',
    'depends_on_past': False,
    'email': ['ga.diaz.mendez@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}   



with DAG(
    'tutorial_EDIT',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2021,5,28,10,52,0),
    tags=['example_EDIT'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='create',
        bash_command= f'touch {filepath}',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 1m',
        retries=3,
    )

    t1 >> t2