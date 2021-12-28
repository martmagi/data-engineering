import datetime
import json
import numpy as np
import pandas as pd
import pdpipe as pdp

import requests
from airflow import DAG
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

def first_clean(input_file: str, output_folder: str):
    df=pd.read_json(f"{input_file}", orient='records')

    df[['status','origin','year']]=np.nan
    for i in range(len(df.details)):
        df.status[i]=df.details[i]['status']
        df.origin[i]=df.details[i]['origin']
        df.year[i]=df.details[i]['year']

    drop_columns=pdp.ColDrop(columns=["last_update_source","category","template_image_url", "ld", "additional_references", "search_keywords", "meta","details"])
    remove_duplicates=pdp.DropDuplicates(["title", "url"])
    drop_unconfirmed=pdp.ValKeep(values=["confirmed"], columns=["status"])
    pipeline_1=pdp.PdPipeline([drop_columns,remove_duplicates,drop_unconfirmed])
    df_2=pipeline_1(df)
    js = df_2.to_json(orient = 'columns')

    with open('cleaned_1.json', 'w', encoding='utf-8') as f:
        json.dump(js, f, ensure_ascii=False, indent=4)

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

second_node = PythonOperator(
    task_id='first_clean',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=first_clean,
    op_kwargs={
        "input_file": "memes_raw_data.json",
        "output_file": DAGS_FOLDER,
    },
    depends_on_past=True,
)

final_dummy_node = DummyOperator(
    task_id='finale',
    dag=assignment_dag,
    trigger_rule='none_failed'
)

# Run the DAGs
first_node >> second_node >> final_dummy_node
