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
   
   if bucket.exists():
    blobs = bucket.list_blobs()

    for blob in blobs:
        print(f"Deleting {blob.name}...")
        blob.delete()
   else:
      bucket = client.create_bucket(bucket_name)
      print(f"Bucket {bucket.name} created.")

def create_temp_table(project_id, orig_dataset_id, temp_dataset_id, view_ref):
   """Create a temp table for exporting the results of a view to GCS"""

   

   client = bigquery.Client()

   query = f"""
            CREATE OR REPLACE TABLE `{project_id}.{temp_dataset_id}.{view_ref}` AS
            SELECT * FROM `{project_id}.{orig_dataset_id}.{view_ref}`;
            """
   query_job = client.query(query)
   query_job.result()


def create_dataset_if_not_exists(project_id, dataset_id, location="US"):
    """
    Create a BigQuery dataset if it does not exist.
    """
    client = bigquery.Client(project=project_id)

    # Construct a full dataset ID in the format `project_id.dataset_id`
    full_dataset_id = f"{project_id}.{dataset_id}"

    try:
        # Check if the dataset already exists
        client.get_dataset(full_dataset_id)  # Will raise an exception if not found
        print(f"Dataset '{full_dataset_id}' already exists.")
    except Exception as e:
        if "Not found" in str(e):
            # Dataset does not exist, create it
            dataset = bigquery.Dataset(full_dataset_id)
            dataset.location = location
            client.create_dataset(dataset)
            print(f"Created dataset '{full_dataset_id}' in location '{location}'.")
        else:
            # Reraise other exceptions
            raise e  


def save_bq_table_to_gcs(bucket_name, project_id, dataset_id, table_id, filename):
    # from google.cloud import bigquery
    client = bigquery.Client()
    

    destination_uri = f"gs://{bucket_name}/{filename}"
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
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
        f"Exported {project_id}:{dataset_id}.{table_id} to {destination_uri}"
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
   project_id = os.getenv("TF_VAR_gcp_project")
   dataset_id = "Congress_Target"
   temp_dataset_id = "Congress_Streamlit"

   delete_bucket_contents(bucket_name=bucket_name)
   create_dataset_if_not_exists(project_id=project_id, dataset_id=temp_dataset_id, location="US")
   tables_to_export = ["sponsored_bills_by_member", "votes_by_member"]



   for view in tables_to_export:
    create_temp_table(project_id=project_id,
                      temp_dataset_id=temp_dataset_id,
                      orig_dataset_id=dataset_id,
                      view_ref=view)
    
    save_bq_table_to_gcs(bucket_name=bucket_name,
                            project_id=project_id,
                            dataset_id=temp_dataset_id,
                            table_id=view,
                            filename=f"{view}.parquet")