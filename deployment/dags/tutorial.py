import logging
from datetime import datetime

import pandas as pd
from airflow.models import DagRun
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from sqlalchemy import create_engine

GDRIVE_AUTH_CONFIG = {
    "client_config_backend": "service",
    "service_config": {"client_json_file_path": "/opt/airflow/config/husna-airflow-sa.json"}
}
GOOGLE_DRIVE_FOLDER_ID = '181R-ug68jYWaI9vBttHnHbt7cTQbuZ1s'

CONNECTION_URL = 'postgresql://airflow:airflow@10.10.10.152:5432/test_airflow'

TARGET_SCHEMA = 'public'
TARGET_TABLE_PREFIX = 'test_airflow'


def push_to_database(url: str, schema: str, table: str, connection_url: str):
    frame = pd.read_csv(url)

    logging.info('Pushing to database')

    connection = create_engine(connection_url, client_encoding='utf8')

    frame.to_sql(schema=schema, name=table, if_exists='replace', index=False, con=connection)

    logging.info('Done')


def get_gdrive_file(folder: str, filename:str, settings: dict, dag_run: DagRun = None):
    gauth = GoogleAuth(settings=settings)
    gauth.ServiceAuth()

    drive = GoogleDrive(gauth)

    logging.info(f'Looking for file [{filename}] in folder [{folder}]')
    statement = {'q': f"'{folder}' in parents and title='{filename}' and trashed=false"}
    files = drive.ListFile(statement).GetList()

    if len(files) == 0:
        return None
    if len(files) > 1:
        raise Exception(f"Multiple files found for folder [{folder}] and filename [{filename}]")
    return files[0]['webContentLink']


def check_file_if_exists(file: any):
    if file is None:
        logging.info('File not found')
        return 'end'
    logging.info('File found')
    return 'etl'


with DAG(
        dag_id='husna_dag',
        schedule='@daily',
        start_date=datetime(2023, 10, 1),
        end_date=datetime(2023, 10, 26)
) as dag:
    task_start = EmptyOperator(task_id='start')

    task_get_file = PythonOperator(
        task_id='get_file',
        python_callable=get_gdrive_file,
        op_kwargs={
            'folder': GOOGLE_DRIVE_FOLDER_ID,
            'filename': '{{ ds }}.csv',
            'settings': GDRIVE_AUTH_CONFIG
        }
    )

    task_check_file = BranchPythonOperator(
        task_id='check_file',
        python_callable=check_file_if_exists,
        op_kwargs={
            'file': task_get_file.output
        }
    )

    task_etl = PythonOperator(
        task_id='etl',
        python_callable=push_to_database,
        op_kwargs={
            'url': task_get_file.output,
            'schema': TARGET_SCHEMA,
            'table': f'{TARGET_TABLE_PREFIX}_{{{{ ds_nodash }}}}',
            'connection_url': CONNECTION_URL
        }
    )

    task_end = EmptyOperator(task_id='end')

    task_start >> task_get_file >> task_check_file
    task_check_file >> task_etl >> task_end
    task_check_file >> task_end
