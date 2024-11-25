# fetch_data.py

from google.cloud import bigquery
import pandas as pd
import os
from google.oauth2 import service_account
from google.cloud import storage, bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('TF_VAR_google_credentials')

# credentials = service_account.Credentials.from_service_account_file(creds_path)

def delete_bucket_contents(bucket_name):
    # Get the bucket
   client = storage.Client()
   bucket = client.bucket(bucket_name)

    # List and delete all objects in the bucket
   blobs = bucket.list_blobs()

   for blob in blobs:
    print(f"Deleting {blob.name}...")
    blob.delete()

def save_bq_table_to_gcs(bucket_name, project, dataset_id, table_id, filename):
    # from google.cloud import bigquery
    client = bigquery.Client()
    

    destination_uri = f"gs://{bucket_name}/{filename}"
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET,  # Set format to PARQUET
        compression=bigquery.Compression.SNAPPY               
    )

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config=extract_job_config
    )  
    extract_job.result()  # Waits for job to complete.

    print(
        f"Exported {project}:{dataset_id}.{table_id} to {destination_uri}"
    )


def fetch_data_from_bigquery(query, date_cols=[]):
    client = bigquery.Client()
    query_job = client.query(query)
    df = query_job.to_dataframe()

    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    return df



def save_data(df, file_path):
    df.to_parquet(file_path)


if __name__ == "__main__":
    
    # bills_by_member_query = """
    #                         SELECT *
    #                         FROM Congress.vw_sponsored_bills_by_member
    #                         """
    # bills_by_member_df = fetch_data_from_bigquery(bills_by_member_query, date_cols=['introducedDate'])
    # save_data(bills_by_member_df, "streamlit_data/sponsored_bills_by_member.parquet")

    # votes_by_member_query = """SELECT *
    #                             FROM Congress.votes_by_member"""
    # votes_by_member_df = fetch_data_from_bigquery(votes_by_member_query, date_cols=['vote_date'])
    # save_data(votes_by_member_df, "streamlit_data/votes_by_member.parquet")
   
   bucket_name = os.getenv("streamlit_data_bucket_name")
   project = os.getenv("TF_VAR_gcp_project")
   dataset_id = "Congress_Target"

   delete_bucket_contents(bucket_name=bucket_name)
   tables_to_export = ["sponsored_bills_by_member", "votes_by_member"]

   for table in tables_to_export:
    save_bq_table_to_gcs(bucket_name=bucket_name,
                            project=project,
                            dataset_id=dataset_id,
                            table_id=table,
                            filename=f"{table}.parquet")