from datetime import datetime, timedelta

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryDeleteTableOperator
from airflow import DAG
from airflow.utils.dates import days_ago

from scripts.python.utils import *
from scripts.python.xmlConvert import *
from scripts.python.get_votes import *
from scripts.python.get_members import *
from schemas.external_schemas import house_votes_schema, senate_votes_schema, bill_schema, member_schema, senate_id_schema

import os
import logging


# PROJECT_ID = os.getenv("GCP_PROJECT_ID")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
print(f"Bucket Name: {BUCKET_NAME}")

CONGRESS_API_KEY = os.environ.get("CONGRESS_API_KEY")

BIGQUERY_DATASET= 'Congress_Target'

# Importing defined schemas to use when creating BigQuery external tables
DATA_TYPES = {'bills'  : bill_schema,
              'house_votes'  : house_votes_schema,
              'senate_votes' : senate_votes_schema,
              'members'      : member_schema,
              'senate_ids': senate_id_schema
              }
              
# Define congresses for which to fetch data
congress_numbers = ['118', '119']

# Define bill types to download
bill_types = ['sres', 'hr', 'hconres', 'hjres', 'hres', 's', 'sjres', 'sconres']

# Parameters for members API call
MEMBERS_START_DATE = "2023-01-01T00:00:00Z" # Start of 118th Congress
MEMBERS_END_DATE = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
params_members = {
    'limit': 250,
    'offset': 0,
    'start_date': MEMBERS_START_DATE,
    "end_date":MEMBERS_END_DATE,
    'api_key': CONGRESS_API_KEY,
    'end_year_limit': 2015
}

# Dynamically create bash command that will be used to download bulk bill statuses
BASE_URL = 'https://www.govinfo.gov/bulkdata/BILLSTATUS/'
bash_command = ' && '.join([
    f'rm -rf /opt/airflow/dags/data/bills/{congress_number}/{bill_type} \
    && mkdir -p /opt/airflow/dags/data/bills/{congress_number}/{bill_type} \
    && wget -P /opt/airflow/dags/data/bills/{congress_number}/{bill_type} "{BASE_URL}{congress_number}/{bill_type}/BILLSTATUS-{congress_number}-{bill_type}.zip" \
    && unzip /opt/airflow/dags/data/bills/{congress_number}/{bill_type}/BILLSTATUS-{congress_number}-{bill_type} -d /opt/airflow/dags/data/bills/{congress_number}/{bill_type} \
    && rm /opt/airflow/dags/data/bills/{congress_number}/{bill_type}/BILLSTATUS-{congress_number}-{bill_type}.zip \
    || echo "Failed to download or unzip /opt/airflow/dags/data/bills/{congress_number}/{bill_type}"' \
    for congress_number in congress_numbers for bill_type in bill_types
])


# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": days_ago(1),
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

start = DummyOperator(
    task_id='start_dummy',
    dag=dag
)

# Define the BashOperator to run wget for each file
get_bills = BashOperator(
    task_id = 'download_bill_statuses',
    bash_command=bash_command,
    dag=dag
)

convert_to_json = PythonOperator(
    task_id = 'convert_bill_files',
    python_callable=convert_folder_xml_to_newline_json,
    op_kwargs={'folder':'/opt/airflow/dags/data/bills',
               'filter_files':True,
               'project_id':PROJECT_ID,
               'dataset_id':BIGQUERY_DATASET,
               'table_id':'dim_bills'
               }
)

get_votes_from_bills = PythonOperator(
    task_id = 'get_votes_from_downloaded_bills',
    python_callable = get_votes_for_saved_bills,
    op_kwargs={'local_folder_path':'/opt/airflow/dags/data/bills',
               'start_date_task': 'start_dummy',
               'save_folder':'/opt/airflow/dags/data/votes',
               'bucket_name':BUCKET_NAME,
               'project_id':PROJECT_ID,
               'dataset_id':BIGQUERY_DATASET,
               'table_id':'dim_votes'},
    provide_context=True
)

upload_bills_to_gcs = PythonOperator(
    task_id = 'upload_bills_to_gcs',
    python_callable=upload_folder_to_gcs,
    op_kwargs={'local_folder_path':'/opt/airflow/dags/data/bills', 
               'bucket_name':BUCKET_NAME, 
               'destination_folder':'bills',
               'run_date_subfolder':True,
               'remove_local':True}
)


get_members_data = PythonOperator(
    task_id = 'get_members_data',
    python_callable=get_members,
    op_kwargs= {'params':params_members,
                'project_id':PROJECT_ID,
                'dataset_id':BIGQUERY_DATASET,
                'table_id':'dim_members',
                'bucket_name':BUCKET_NAME},
    dag=dag
)

get_senate_id_data = PythonOperator(
     task_id='get_senate_id_data',
     python_callable=get_senate_ids,
     op_kwargs={'save_folder':'senate_ids', 
                'bucket_name':BUCKET_NAME},
     dag=dag
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /usr/app/dbt && dbt deps && dbt seed && dbt run',
    dag=dag,
)


for data_type in DATA_TYPES.keys():

    delete_external_table = BigQueryDeleteTableOperator(
            task_id=f'delete_{data_type}_external_table',
            deletion_dataset_table=f'{PROJECT_ID}.Congress_Stg.{data_type}_External',
            ignore_if_missing=True,  # Ignore if the table doesn't exist
            gcp_conn_id='google_cloud_default',
            dag=dag
        )

    create_external_table = BigQueryCreateExternalTableOperator(task_id = f'create_{data_type}_external_table',
                                                                bucket=BUCKET_NAME,
                                                                destination_project_dataset_table=f'{PROJECT_ID}.Congress_Stg.{data_type}_External',
                                                                source_objects=[f"{data_type}/*.json"],
                                                                source_format='NEWLINE_DELIMITED_JSON',
                                                                schema_fields=DATA_TYPES[data_type],
                                                                gcp_conn_id='google_cloud_default',
                                                                dag=dag)
    
    if data_type == 'bills':
        upload_bills_to_gcs >> delete_external_table >> create_external_table
    elif data_type in [ 'house_votes', 'senate_votes']:
        get_votes_from_bills >> delete_external_table >> create_external_table
    elif data_type in ['members', 'senate_ids']:
           get_senate_id_data >> delete_external_table >> create_external_table

    create_external_table >> dbt_run



start >> get_bills >> convert_to_json >> get_votes_from_bills >> upload_bills_to_gcs
start >> get_members_data >> get_senate_id_data




