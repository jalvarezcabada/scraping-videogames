from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#Argumentos por defecto de nuestro DAG
default_args = {
    'owner': 'Joaquin Alvarez Cabada',
    'start_date': datetime(2021,10,11),
    'email': ['<<email>>'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    #'wait_for_downstream': True
}

with DAG(
    dag_id='scraping_games',
    schedule_interval='0 19 * * 1',
    default_args=default_args,
    description='ETL process',
    catchup=False
) as dag:

    #Comandos Bash a ejecutar dentro de nuestro servidor
    popularity = BashOperator(
        task_id = 'popularity',
        bash_command= "/home/ubuntu/GitHub/scraping/env/bin/python /home/ubuntu/GitHub/scraping/main.py --process popularity --type requests --location local",
        dag= dag
    )

    metadata = BashOperator(
        task_id = 'metadata',
        bash_command= "/home/ubuntu/GitHub/scraping/env/bin/python /home/ubuntu/GitHub/scraping/main.py --process metadata_content --location local",
        dag= dag
    )

    popularity >> metadata