import datetime
import json

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

DAGS_FOLDER = '/opt/airflow/dags/'
REQUEST_URL = 'https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json'

default_args_dict = {
    'start_date': datetime.datetime(2021, 11, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "35 12 * * 5",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

assignment_dag = DAG(
    dag_id='assignment_dag',
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


first_node = PythonOperator(
    task_id='get_dataset',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=get_dataset,
    op_kwargs={
        "output_folder": DAGS_FOLDER,
        "url": REQUEST_URL,
    },
    depends_on_past=False,
)

run_jar_task = BashOperator(
    task_id='runjar',
    trigger_rule='none_failed',
    dag=assignment_dag,
    bash_command='java -cp /talend-files/cleansing.jar'
)

final_dummy_node = DummyOperator(
    task_id='finale',
    dag=assignment_dag,
    trigger_rule='none_failed'
)

# Run the DAGs
first_node >> run_jar_task >> final_dummy_node
