import json
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow import DAG

from scripts.python.get_data import get_and_upload_data, get_bills_from_bq, upload_folder_to_gcs, get_votes_for_saved_bills
from scripts.python.xmlConvert import convert_folder_xml_to_newline_json
from schemas.external_schemas import *

from dotenv import load_dotenv
import os
import logging

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET= 'Congress'
data_types = ['bills', 'votes']
# schemas = {'bills':bill_status_ddl,
#            'votes':vote_schema}
# date_cols = {'bills':bill_status_ddl,
#            'votes':vote_schema}

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


# bash_command = ' && '.join([
#                             f'wget {file_url} && \
#                             unzip -o /{file_url.split("/")[-1]} && \
#                             rm {file_url.split("/")[-1]} || \
#                             echo "Failed to download or unzip {file_url}"' \
#                             for file_url in files_to_download])

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

get_votes = PythonOperator(
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

upload_to_gcs = PythonOperator(
    task_id = 'upload_bills_to_gcs',
    python_callable=upload_folder_to_gcs,
    op_kwargs={'local_folder_path':'/opt/airflow/dags/data/bills', 
               'bucket_name':'congress_data', 
               'destination_folder':'bills'}
)

bigquery_external_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_create_bill_status_external_table_task",
        configuration={
            "query": {
                "query": bill_status_ddl,
                "useLegacySql": False,
            }
        },
        dag=dag  
    )

CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.bill_status \
        PARTITION BY latestActionDate_formatted \
        AS \
        SELECT *, DATE(bill.latestAction.actionDate) latestActionDate_formatted \
        FROM {BIGQUERY_DATASET}.bill_status_external_table;"
    )

bq_create_partitioned_table_job = BigQueryInsertJobOperator(
    task_id=f"bq_create_bills_{BIGQUERY_DATASET}_partitioned_table_task",
    configuration={
        "query": {
            "query": CREATE_BQ_TBL_QUERY,
            "useLegacySql": False,
        }
    },
    dag=dag  
)

# bigquery_external_table_task = BigQueryCreateExternalTableOperator(
#         task_id = "bill_status_external_table_task",
#         table_resource={
#             "tableReference": {
#                 "projectID": PROJECT_ID,
#                 "datasetId": BIGQUERY_DATASET,
#                 "tableId": f"bill_status_external_table",
#             },
#             "externalDataConfiguration": {
#                 "autodetect": "False",
#                 "schema": {
#                     "fields": bill_status_schema
#                 },
#                 "sourceFormat": 'JSON',
#                 "sourceUris": ['gs://congress_data/bills/hconres/*.json',
#                                 'gs://congress_data/bills/hjres/*.json',
#                                 'gs://congress_data/bills/hr/*.json',
#                                 'gs://congress_data/bills/s/*.json',
#                                 'gs://congress_data/bills/sjres/*.json',
#                                 'gs://congress_data/bills/sres/*.json'],
#                 },
#         },
#         dag=dag 
#     )

get_bills >> convert_to_json >> upload_to_gcs >> bigquery_external_table_task >> bq_create_partitioned_table_job

convert_to_json >> get_votes

