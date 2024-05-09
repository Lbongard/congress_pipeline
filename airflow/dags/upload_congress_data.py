import json
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow import DAG

from scripts.python.get_data import *
from scripts.python.xmlConvert import convert_folder_xml_to_newline_json
from schemas.external_schemas import *

from dotenv import load_dotenv
import os
import logging


# PROJECT_ID = os.getenv("GCP_PROJECT_ID")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
print(f"Bucket Name: {BUCKET_NAME}")

CONGRESS_API_KEY = os.environ.get("CONGRESS_API_KEY")

BIGQUERY_DATASET= 'Congress'
DATA_TYPES = {'bill_status':{'table_struct':bill_status_ddl,
                             'date_col':'bill.introducedDate',
                             'local_folder_path':'/opt/airflow/dags/data/bills'},
              'votes':      {'table_struct':vote_ddl,
                             'date_col':'vote.date',
                             'local_folder_path':'/opt/airflow/dags/data/votes'},
              'members':    {'table_struct':member_ddl,
                             'date_col':'updateDate',
                             'local_folder_path':'/opt/airflow/dags/data/members'}
                             }

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2024, 3, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}



dag = DAG(
    "upload_congress_data",
    default_args=default_args,
    schedule_interval="@once",
    max_active_runs=1,
)

files_to_download = [
    ['sres', 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/sres/BILLSTATUS-118-sres.zip'],
    ['hr', 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/hr/BILLSTATUS-118-hr.zip'],
    ['hconres', 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/hconres/BILLSTATUS-118-hconres.zip'],
    ['hjres', 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/hjres/BILLSTATUS-118-hjres.zip'],
    ['s', 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/s/BILLSTATUS-118-s.zip'],
    ['sjres', 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/sjres/BILLSTATUS-118-sjres.zip'],
    ['sconres', 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/sconres/BILLSTATUS-118-sconres.zip']
]


bash_command = ' && '.join([
    f'rm -rf /opt/airflow/dags/data/bills/{bill_type} \
    && mkdir -p /opt/airflow/dags/data/bills/{bill_type} \
    && wget -P /opt/airflow/dags/data/bills/{bill_type} "{file_url}" \
    && unzip /opt/airflow/dags/data/bills/{bill_type}/{file_url.split("/")[-1]} -d /opt/airflow/dags/data/bills/{bill_type} \
    && rm /opt/airflow/dags/data/bills/{bill_type}/{file_url.split("/")[-1]} \
    || echo "Failed to download or unzip {file_url}"' \
    for bill_type, file_url in files_to_download
])


# Define the BashOperator to run wget for each file
get_bills = BashOperator(
    task_id = 'download_bill_statuses',
    bash_command=bash_command,
    dag=dag
)

get_votes_from_bills = PythonOperator(
    task_id = 'get_votes_from_downloaded_bills',
    python_callable = get_votes_for_saved_bills,
    op_kwargs={'local_folder_path':'/opt/airflow/dags/data/bills',
               'save_folder':'/opt/airflow/dags/data/votes'}
)

convert_to_json = PythonOperator(
    task_id = 'convert_bill_files',
    python_callable=convert_folder_xml_to_newline_json,
    op_kwargs={'folder':'/opt/airflow/dags/data/bills'}
)

def format_end_date(**kwargs):
    execution_date = kwargs['execution_date']
    end_date = execution_date.strftime('%Y-%m-%dT00:00:00Z')

    logging.info("Formatted execution date: %s", end_date)
    return end_date

params_members = {
    'limit': 250,
    'offset': 0,
    # Beginning of 118th Congress
    'start_date': "2023-01-01T00:00:00Z",
    "end_date":"{{ task_instance.xcom_pull(task_ids='format_execution_date_task') }}",
    'api_key': CONGRESS_API_KEY
}

format_date_task = PythonOperator(
    task_id='format_execution_date_task',
    python_callable=format_end_date,
    provide_context=True,
    dag=dag
)

get_members_data = PythonOperator(
    task_id = 'get_members_data',
    python_callable=get_members,
    op_kwargs= {'params':params_members,
                'save_folder':'/opt/airflow/dags/data/members'},
    dag=dag
)


for data_type in DATA_TYPES.keys():

    upload_to_gcs = PythonOperator(
    task_id = f'upload_{data_type}_to_gcs',
    python_callable=upload_folder_to_gcs,
    op_kwargs={'local_folder_path':DATA_TYPES[data_type]['local_folder_path'], 
               'bucket_name':BUCKET_NAME, 
               'destination_folder':f'{data_type}'}
)
    
    bigquery_external_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_{data_type}_external_table_task",
            configuration={
                "query": {
                    "query": DATA_TYPES[data_type]['table_struct'],
                    "useLegacySql": False,
                }
            },
            dag=dag  
        )
    
    date_col = DATA_TYPES[data_type]['date_col']
    new_date_col = date_col.split('.')[-1] + '_formatted'
    
    CREATE_BQ_TBL_QUERY = (
        f"DROP TABLE IF EXISTS {BIGQUERY_DATASET}.{data_type}; \
        CREATE TABLE {BIGQUERY_DATASET}.{data_type} \
        PARTITION BY {new_date_col} \
        AS \
        SELECT *, DATE({date_col}) {new_date_col} \
        FROM {BIGQUERY_DATASET}.{data_type}_external_table;"
    )

    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{BIGQUERY_DATASET}_{data_type}_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        },
        dag=dag  
    )
    
    upload_to_gcs >> bigquery_external_table_task >> bq_create_partitioned_table_job

    if data_type == 'bill_status':
        convert_to_json >> upload_to_gcs
    elif data_type == 'votes':
        get_votes_from_bills >> upload_to_gcs
    elif data_type == 'members':
        get_members_data >> upload_to_gcs

get_bills >> convert_to_json >> upload_to_gcs >> bigquery_external_table_task >> bq_create_partitioned_table_job

convert_to_json >> get_votes_from_bills

format_date_task >> get_members_data


