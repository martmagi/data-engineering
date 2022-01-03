import datetime
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from augmentation import augment
from download_datasets import get_raw_dataset, get_vision_dataset
from mongo_upload import upload_to_mongo
from transformation import transform
from cleaning import clean

DAGS_FOLDER = '/opt/airflow/dags/'
KYM_FILE_PATH = os.path.join(DAGS_FOLDER, 'kym.json')
VISION_FILE_PATH = os.path.join(DAGS_FOLDER, 'kym_vision.json')
CLEAN_FILE_PATH = os.path.join(DAGS_FOLDER, 'kym_clean.json')
AUGMENTED_FILE_PATH = os.path.join(DAGS_FOLDER, 'kym_augment.json')
TRANSFORMED_FILE_PATH = os.path.join(DAGS_FOLDER, 'kym_transform.json')

default_args_dict = {
    'start_date': datetime.datetime(2022, 1, 1, 0, 0, 0),
    'schedule_interval': None
}

default_dag = DAG(
    dag_id='Data_Engineering_Project_G12',
    default_args=default_args_dict,
    template_searchpath=DAGS_FOLDER
)

get_raw_dataset_task = PythonOperator(
    task_id='get_raw_dataset',
    dag=default_dag,
    python_callable=get_raw_dataset,
    op_kwargs={
        "kym_file_path": KYM_FILE_PATH
    }
)

cleaning_task = PythonOperator(
    task_id='clean',
    dag=default_dag,
    python_callable=clean,
    op_kwargs={
        "kym_file_path": KYM_FILE_PATH,
        "clean_file_path": CLEAN_FILE_PATH
    }
)

get_vision_dataset_task = PythonOperator(
    task_id='get_vision_dataset',
    dag=default_dag,
    python_callable=get_vision_dataset,
    op_kwargs={
        "vision_file_path": VISION_FILE_PATH
    }
)

augmentation_task = PythonOperator(
    task_id='augment',
    dag=default_dag,
    python_callable=augment,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs={
        "clean_file_path": CLEAN_FILE_PATH,
        "vision_file_path": VISION_FILE_PATH,
        "augmented_file_path": AUGMENTED_FILE_PATH
    }
)

transformation_task = PythonOperator(
    task_id='transform',
    dag=default_dag,
    python_callable=transform,
    op_kwargs={
        "augmented_file_path": AUGMENTED_FILE_PATH,
        "transformed_file_path": TRANSFORMED_FILE_PATH
    }
)

mongo_upload_task = PythonOperator(
    task_id='mongo_upload',
    dag=default_dag,
    python_callable=upload_to_mongo,
    op_kwargs={
        "transformed_file_path": TRANSFORMED_FILE_PATH
    }
)

neo4j_task = DummyOperator(
    task_id='run_neo4j',
    dag=default_dag
)

# Run the tasks.
[get_raw_dataset_task >> cleaning_task, get_vision_dataset_task] >> augmentation_task >> transformation_task >> [mongo_upload_task, neo4j_task]
