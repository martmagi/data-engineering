import datetime
import json

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DAGS_FOLDER = '/opt/airflow/dags/'
REQUEST_URL = 'https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json'
TALEND_JAR_URL = 'https://github.com/martmagi/data-engineering/blob/main/talend_files/cleansing_1.jar'

default_args_dict = {
    'start_date': datetime.datetime(2021, 11, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "35 12 * * 5",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

default_dag = DAG(
    dag_id='default_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=DAGS_FOLDER,
    schedule_interval="35 12 * * 5",
)


def get_dataset(output_folder: str, url: str):
    url = f"{url}"
    memes_raw_data = requests.get(url).json()
    with open(f'{output_folder}/memes_raw_data.json', 'w') as f:
        json.dump(memes_raw_data, f, ensure_ascii=False)


def get_talend_jar(output_folder: str, url: str):
    url = f"{url}"
    talend_jar_file = requests.get(url)
    with open(f'{output_folder}/talend.jar', 'wb') as f:
        f.write(talend_jar_file.content)


get_dataset_task = PythonOperator(
    task_id='get_dataset',
    dag=default_dag,
    python_callable=get_dataset,
    op_kwargs={
        "output_folder": DAGS_FOLDER,
        "url": REQUEST_URL,
    }
)

get_talend_jar_task = PythonOperator(
    task_id='get_talend_jar',
    dag=default_dag,
    python_callable=get_talend_jar,
    op_kwargs={
        "output_folder": DAGS_FOLDER,
        "url": TALEND_JAR_URL,
    }
)

run_talend_jar = BashOperator(
    task_id='run_talend_jar',
    trigger_rule='none_failed',
    dag=default_dag,
    bash_command='java -jar talend.jar'
)

# Run the operators
get_dataset_task >> get_talend_jar_task >> run_talend_jar
