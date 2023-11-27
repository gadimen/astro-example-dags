
from datetime import timedelta
from textwrap import dedent
from datetime import datetime, timedelta
import json
from faker import Faker
from random import randint
import logging
fake = Faker('en_US')


# Objeto DAG
from airflow import DAG

# Operador de Python
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")


# Argumentos padrão
default_args = {
    'owner': 'german Mendez',
    'depends_on_past': False,
    'email': ['ga.diaz.mendez@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}   

# inicializar o objeto DAG com a informação do pipeline
with DAG(
    'tutorial_EDIT_python',
    default_args=default_args,
    description='DAG tutorial Python',
    schedule_interval=None,
    start_date=datetime(2021,5,28),
    tags=['example_EDIT','PythonOperator','turma-2021-2'],
) as dag:

    # simular dados: simular o process de extract
    def extract(**kwargs):
        ti = kwargs['ti']
        my_dict = {}
        for _ in range(10):
            my_dict[fake.name()] =  randint(0, 100)
        ti.xcom_push('order_data', my_dict)

    # fazer um transformação , neste caso soma do valor total
    def transform(**kwargs):
        ti = kwargs['ti']
        xtract_data_string = ti.xcom_pull(task_ids='extract', key='order_data')
        logger.info(xtract_data_string)
        order_data = xtract_data_string
        # Tranformar  
        total_order_value = 0
        for value in order_data.values():
            total_order_value += value
        # agregar
        total_value = {"total_order_value": total_order_value}
        total_value_json_string = json.dumps(total_value)
        # passar a seguinte task
        ti.xcom_push('total_order_value', total_value_json_string)

    # Guardar a informação
    def load(**kwargs):
        now = datetime.now().strftime("%d-%m-%Y_%H_%M_%S")
        ti = kwargs['ti']
        print(ti)
        total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
        total_order_value = json.loads(total_value_string)
        with open('/home/german/example_2/ETL_'+now+'.txt', 'w') as outfile:
            json.dump(total_order_value, outfile)
        print(total_order_value)

    # orquestrar as definições
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    # documentação sobre a tarefa específica 
    extract_task.doc_md = dedent(
        """\
    #### Tarefa e extração 
    processo de extract consiste em simular os dados num dicinário, como se 
    estivesse a ler a uma base de dados. É utilizado una Library para simular
    A informação será passada a seguinte task, através do sistema de comunidação 
    de tarefas Xcom
    """
    )

    # task de transformação
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    # documentação do transform
    transform_task.doc_md = dedent(
        """\
    #### Tarefa de transformação
    recebe da tarefa de extrack o Json, 
    Depois soma todos os valores do diccionário e depois cria um novo dict 
    com a soma dos valores totáis.
    A informação é depois enviada à tarefa Load para assim processar e guardar os 
    dados
    """
    )

    # task de load
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    Tarefa que recebe os dados transformados (agregação) e guarda esta 
    informação num JSON na pasta selecionada.
     """
    )

    extract_task >> transform_task >> load_task







