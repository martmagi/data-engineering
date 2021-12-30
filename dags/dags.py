import datetime
import json
import numpy as np
import pandas as pd
import pdpipe as pdp
import pymongo

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DAGS_FOLDER = '/opt/airflow/dags/'
KYM_FILE_PATH = DAGS_FOLDER + 'kym.json'
CLEAN_FILE_PATH = DAGS_FOLDER + 'kym_clean.json'
REQUEST_URL = 'https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json'

default_args_dict = {
    'start_date': datetime.datetime(2021, 11, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "35 12 * * 5",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

default_dag = DAG(
    dag_id='Data_Engineering_Project_G12',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=DAGS_FOLDER,
    schedule_interval="35 12 * * 5",
)


def get_dataset(kym_file_path: str, url: str):
    memes_raw_data = requests.get(f"{url}").json()
    with open(f"{kym_file_path}", 'w') as f:
        json.dump(memes_raw_data, f, ensure_ascii=False)


def first_clean(kym_file_path: str, clean_file_path: str):
    df=pd.read_json(f"{kym_file_path}", orient='records')

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

    with open(f"{clean_file_path}", 'w', encoding='utf-8') as f:
        json.dump(js, f, ensure_ascii=False)

    client = pymongo.MongoClient('mongodb+srv://data-engineering-g12:<password>>@data-engineering-g12.wvade.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    db = client.memesDB
    results_list = df_2.to_dict(orient='records')
    db.memes.insert_many(results_list)


get_dataset_task = PythonOperator(
    task_id='get_dataset',
    dag=default_dag,
    python_callable=get_dataset,
    op_kwargs={
        "kym_file_path": KYM_FILE_PATH,
        "url": REQUEST_URL
    }
)

cleaning_task = PythonOperator(
    task_id='first_clean',
    dag=default_dag,
    python_callable=first_clean,
    op_kwargs={
        "kym_file_path": KYM_FILE_PATH,
        "clean_file_path": CLEAN_FILE_PATH
    }
)

# Run the tasks
get_dataset_task >> cleaning_task
