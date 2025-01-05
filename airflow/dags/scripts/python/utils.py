import os
from google.cloud import storage, bigquery
import logging
from datetime import datetime

def upload_folder_to_gcs(local_folder_path, bucket_name, destination_folder, run_date_subfolder=True, remove_local=True, **kwargs):
    """
    Uploads the contents of a local folder to a Google Cloud Storage (GCS) bucket. Optionally removes local

    Args:
        local_folder_path (str):             Path to the local folder containing files to upload.
        bucket_name (str):                   Name of the GCS bucket.
        destination_folder (str):            Destination subfolder in the GCS bucket.
        run_date_subfolder (bool, optional): Whether to create a subfolder in GCS named with the Airflow run date. Defaults to True.
        remove_local (bool, optional):       Whether to delete the local files after uploading. Defaults to True.

    Returns:
        None
    """
    
    if run_date_subfolder:
        # Get run date to name subfolder in gcs for this run
        ti = kwargs['ti']
        run_date = ti.execution_date.date()
        destination_folder = f'{destination_folder}/{run_date}'

    remove_gcs_folder_if_exists(bucket_name, destination_folder)
    
    # Initialize a clientdock
    client = storage.Client()

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # Iterate over files in the local folder
    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            # Construct the full local path
            local_file_path = os.path.join(root, file)
            
            # Determine the destination path in GCS
            gcs_blob_name = os.path.join(destination_folder, os.path.relpath(local_file_path, local_folder_path))
            
            # Upload the file to GCS
            blob = bucket.blob(gcs_blob_name)
            
            # Delete file if it exists
            if blob.exists():
                blob.delete()

            try:
                blob.upload_from_filename(local_file_path)
                logging.info(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_blob_name}")
                if remove_local:
                    os.remove(local_file_path)
            except Exception as e:
                logging.info(f"Failed to upload {local_file_path} to gs://{bucket_name}/{gcs_blob_name}")


def upload_to_gcs_from_string(object, bucket_name, destination_blob_name):
    """
    Uploads an object (string content) to a Google Cloud Storage (GCS) bucket.

    Args:
        object (str):                The string content to upload.
        bucket_name (str):           GCS bucket for uploading records
        destination_blob_name (str): Destination path and filename in the GCS bucket.

    Returns:
        None
    """
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    # Upload the contents of the buffer to GCS
    blob.upload_from_string(object)

    logging.info(f'File {destination_blob_name} uploaded to {bucket_name}.')


def get_max_updated_date(date_field, project_id, dataset_id, table_id, default_date='2015-01-01T00:00:00Z', format='%Y-%m-%dT%H:%M:%SZ'):

    """
    Retrieves the maximum value of a date field from a BigQuery table.

    Args:
        date_field (str):             Name of the date field in the table.
        project_id (str):             GCP project ID.
        dataset_id (str):             BigQuery dataset ID.
        table_id (str):               BigQuery table ID.
        default_date (str, optional): Default date to return if no data is found. Defaults to '2015-01-01T00:00:00Z'.
        format (str, optional):       Date format for parsing and returning the date. Defaults to '%Y-%m-%dT%H:%M:%SZ'.

    Returns:
        datetime: Maximum date from the table or the default date if no data is found.
        """
    
    client = bigquery.Client()

    # Query to get the max updated_date
    query = f"""
    SELECT MAX(
                COALESCE({date_field}, '{default_date}')
                    ) AS max_updated_date
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    
    try:
        # Run the query
        result = client.query(query).result()
        
        # Extract the max updated_date
        for row in result:
            max_updated_date = row.max_updated_date
        
        # If there is no data in the table, return default date
        if max_updated_date is None:
            return datetime.strptime(default_date, format)
        else:
            return datetime.strptime(max_updated_date, format)
    
    except Exception as e:
        # If the table doesn't exist or any error occurs, return default date
        logging.info(f"Error querying max updated_date: {e}")
        return default_date
    

def remove_gcs_folder_if_exists(bucket_name, folder_path):
    """
    Deletes all blobs within a folder in a Google Cloud Storage (GCS) bucket, if the folder exists.

    Args:
        bucket_name (str): Name of the GCS bucket.
        folder_path (str): Path to the folder in the bucket to be removed.

    Returns:
        None
    """
    # Initialize a GCS client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    logging.info(f'Checking {folder_path} in {bucket_name}')

    # List all blobs under the given folder path
    blobs = list(bucket.list_blobs(prefix=folder_path))
    logging.info(f'Blobs found: {blobs}')

    # If blobs exist, delete them
    if blobs:
        for blob in blobs:
            blob.delete()
            logging.info(f'Blob {blob.name} deleted from GCS.')

def replace_special_characters(data):
    """
    Replaces special characters in dictionary keys with safe characters.

    Supported replacements:
        - '-' replaced with '_'
        - '@' removed
        - '#' removed
        - ':' removed

    Args:
        data (dict | list | any): Input data to process. Can be a dictionary, list, or other types.

    Returns:
        dict | list | any: Processed data with special characters replaced.
    """
    if isinstance(data, dict):
        new_dict = {}
        for key, value in data.items():
            
            new_key = key.replace('-', '_').\
                replace('@', ''). \
                replace('#',''). \
                replace(':', '')  
            
            new_dict[new_key] = replace_special_characters(value)  # Recursively process nested dictionaries
        return new_dict
    elif isinstance(data, list):
        return [replace_special_characters(item) for item in data]  # Recursively process items in lists
    else:
        return data 
    

def ensure_item_is_list(json_data):
    """
    Recursively ensure that any key named 'item' is a list. 
    If 'item' is a dictionary, it is converted to a list containing that dictionary.

    Args:
        json_data (dict | list): Input JSON data to process. Can be a dictionary or list.

    Returns:
        dict | list: JSON data with 'item' keys converted to lists if needed.

    """
    if isinstance(json_data, dict):
        # Iterate through the dictionary
        for key, value in json_data.items():
            if key == 'item':
                # If 'item' is not a list, convert it into a list
                if not isinstance(value, list):
                    json_data[key] = [value]  # Convert single dict to list
            # Recurse into the value if it is a nested dict or list
            ensure_item_is_list(value)

    elif isinstance(json_data, list):
        # Iterate through the list elements and apply the function recursively
        for i in range(len(json_data)):
            ensure_item_is_list(json_data[i])

    return json_data