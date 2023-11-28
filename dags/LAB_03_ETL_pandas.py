
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from datetime import timedelta
from textwrap import dedent
from datetime import datetime, timedelta
import json
from faker import Faker
from random import randint
import logging
import pandas as pd
import glob
fake = Faker('en_US')


# Objeto DAG

# Operador de Python


logger = logging.getLogger("airflow.task")


# Argumentos padrão
default_args = {
    'owner': 'german Mendez',
    'depends_on_past': False,
    'email': ['ga.diaz.mendez@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

file_prefix = "home/german/aula_5/"
file_suffix = ".csv"


# inicializar o objeto DAG com a informação do pipeline
with DAG(
    'tutorial_EDIT_pandas',
    default_args=default_args,
    description='DAG_tutorial_Python',
    schedule_interval=None,
    start_date=datetime(2021, 5, 28),
    tags=['example_EDIT', 'PythonOperator',
          'turma-2021-1', 'Pandas', 'Dynamic'],
) as dag:

    def read_csv(**kwargs):
        fnum = kwargs['file_num']
        fname = "aula_5"+str(i)+'.csv'
        df = pd.read_csv(fname)
        df = df[['case_number', 'date', 'description']]
        df = df.groupby('description', as_index=False)[['case_number']].count()
        df.to_csv('/home/german/aula_5_stagging/agg'+str(fnum)+'.csv')

    def read_and_join(**kwargs):
        frames = []
        for i in glob.glob('./*.csv'):
            frames.append(pd.read_csv(i))
        df = pd.concat(frames)
        df = df['case_number'].sum()
        df = pd.DataFrame([df], columns=['total sum'])
        df.to_csv('/home/german/final_sum.csv')

    tasks = []
    for i in range(0, 10):
        read_pandas = PythonOperator(
            task_id='read_csv_'+str(i),
            python_callable=read_csv,
            op_kwargs={'file_num': i})
        tasks.append(read_pandas)

    Dum = DummyOperator(task_id='A')

    agg_task = PythonOperator(
        task_id='read_agg_sum',
        python_callable=read_and_join
    )

    tasks >> Dum >> agg_task
