# import json
# from datetime import datetime, timedelta

# from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
# from airflow import DAG

# from scripts.python.get_data import get_and_upload_data, get_bills_from_bq#, get_and_upload_votes
# from schemas.external_schemas import *

# from dotenv import load_dotenv
# import os
# import logging

# DATA_TYPES = ['bills', 'member', 'senate_votes', 'house_votes']
# DATE_COLS = {'bills': 'updateDate',             
#              'member': 'updateDate',
#              'senate_votes': 'vote_date',
#              'house_votes': 'vote_metadata.action_date'}

# schema_dict = {'bills': {'schema': bill_schema, 'format':'PARQUET'},
#                'member': {'schema': member_schema, 'format':'PARQUET'},
#                'senate_votes': {'schema': senate_votes_schema, 'format':'PARQUET'},
#                'house_votes': {'schema': house_vote_schema, 'format':'PARQUET'}}


# load_dotenv()

# PROJECT_ID = os.getenv("GCP_PROJECT_ID")
# BIGQUERY_DATASET= 'Congress'
# # BUCKET = os.getenv("GCP_GCS_BUCKET")

# # DAG definition
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": True,
#     "wait_for_downstream": True,
#     "start_date": datetime(2024, 3, 15),
#     "email": ["airflow@airflow.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 2,
#     "retry_delay": timedelta(minutes=1),
# }

# dag = DAG(
#     "upload_historical_congress_data",
#     default_args=default_args,
#     schedule_interval="@once",
#     max_active_runs=1,
# )


def format_end_date(**kwargs):
    execution_date = kwargs['execution_date']
    end_date = execution_date.strftime('%Y-%m-%dT00:00:00Z')

    logging.info("Formatted execution date: %s", end_date)
    return end_date


format_date_task = PythonOperator(
    task_id='format_execution_date_task',
    python_callable=format_end_date,
    provide_context=True,
    dag=dag
)

# params_bills = {
#     'limit': 250,
#     'offset': 0,
#     'start_date': "2024-03-01T00:00:00Z",
#     'congress': 118,
#     "end_date":"{{ task_instance.xcom_pull(task_ids='format_execution_date_task') }}",
#     'api_key': "WNue8kCDCOIlewAsULgnN8j6SqSgAZjE2sYbPsBb",
#     'max_records': 1000
# }


# upload_historical_bill_data = PythonOperator(
#     dag=dag,
#     python_callable=get_and_upload_data,
#     task_id="upload_historical_bills",
#     op_kwargs= {'params':params_bills,
#                 'data_type':'bills'}
# )

params_members = {
    'limit': 250,
    'offset': 0,
    'start_date': "2023-01-01T00:00:00Z",
    "end_date":"{{ task_instance.xcom_pull(task_ids='format_execution_date_task') }}",
    'api_key': "WNue8kCDCOIlewAsULgnN8j6SqSgAZjE2sYbPsBb"
}

# upload_historical_member_data = PythonOperator(
#     dag=dag,
#     python_callable=get_and_upload_data,
#     task_id="upload_historical_members",
#     op_kwargs= {'params':params_members,
#                 'data_type':'member'}

# )

# query_votes = PythonOperator(dag=dag,
#                                    python_callable=get_bills_from_bq,
#                                    task_id="get_bills_from_bq")

# upload_historical_votes = PythonOperator(dag=dag,
#                                python_callable=get_and_upload_votes,
#                                task_id='get_and_upload_votes_to_gcs',
#                                provide_context=True,
#                                op_kwargs={"params": params_bills})

# for data_type in DATA_TYPES:
    
#     schema = schema_dict[data_type]['schema']
#     file_format = schema_dict[data_type]['format']
#     # if data_type == 'bills':
#     #     schema = bill_schema
#     # elif data_type == 'member':
#     #     schema = member_schema
#     # elif data_type == 'votes':
#     #     schema = vote_schema
    
#     bigquery_external_table_task = BigQueryCreateExternalTableOperator(
#         task_id = f"{data_type}_external_table_task",
#         table_resource={
#             "tableReference": {
#                 "projectID": PROJECT_ID,
#                 "datasetId": BIGQUERY_DATASET,
#                 "tableId": f"{data_type}_external_table",
#             },
#             "externalDataConfiguration": {
#                 "autodetect": "False",
#                 "schema": {
#                     "fields": schema
#                 },
#                 "sourceFormat": file_format,
#                 "sourceUris": [f"gs://congress_data/{data_type}/*"],
#             },
#         },
#         dag=dag 
#     )

#     CREATE_BQ_TBL_QUERY = (
#         f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{data_type} \
#         PARTITION BY {DATE_COLS[data_type]}_formatted \
#         AS \
#         SELECT *, DATE({DATE_COLS[data_type]}) {DATE_COLS[data_type]}_formatted \
#         FROM {BIGQUERY_DATASET}.{data_type}_external_table;"
#     )

#     bq_create_partitioned_table_job = BigQueryInsertJobOperator(
#         task_id=f"bq_create_{data_type}_{BIGQUERY_DATASET}_partitioned_table_task",
#         configuration={
#             "query": {
#                 "query": CREATE_BQ_TBL_QUERY,
#                 "useLegacySql": False,
#             }
#         },
#         dag=dag  
#     )

#     if data_type == 'bills': 
#         bigquery_external_table_task.set_upstream(upload_historical_bill_data)
#         bq_create_partitioned_table_job.set_downstream(query_votes)
#     elif data_type in ['house_votes', 'senate_votes']:
#         bigquery_external_table_task.set_upstream(upload_historical_votes)
#     elif data_type == 'member':
#         bigquery_external_table_task.set_upstream(upload_historical_member_data)

#     bigquery_external_table_task >> bq_create_partitioned_table_job




# format_date_task >> upload_historical_bill_data
# format_date_task >> upload_historical_member_data
# query_votes >> upload_historical_votes


