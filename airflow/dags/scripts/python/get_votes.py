import requests
import os
import json
import pandas as pd
import logging
import xmltodict
from datetime import datetime
from .utils import *


def get_votes_for_saved_bills(local_folder_path, bucket_name, project_id, dataset_id, table_id, default_date='2015-01-01', **kwargs):
   
    """
    Gets votes for specified bills saved locally and uploads to GCS bucket.
    Only fetches records updated after most recent UpdatedDate in specified BigQuery table

    Args:
        local_folder_path (str): Path to locally saved bill jsons
        bucket_name (str):       GCS bucket for uploading records
        project_id (str):        GCP Project ID for GCS upload
        dataset_id (str):        GCP Dataset ID for GCS upload
        table_id (str):          GCP table ID for to check UpdatedDate for limiting query
        default_date (str):      Default date to use for limiting query if table with 'table_id' not found

    Returns:
        None
    """

    # Get run date to name subfolder in gcs for this run
    ti = kwargs['ti']
    run_date = ti.execution_date.date()

    # Get max updated date from BigQuery for filtering API results for incremental load
    filter_date = get_max_updated_date(project_id=project_id, 
                                       dataset_id=dataset_id, 
                                       table_id=table_id, 
                                       default_date=default_date, 
                                       date_field='vote_date', 
                                       format='%Y-%m-%d')

    for root, dirs, files in os.walk(local_folder_path):

        # For each directory (root), get the relative path
        save_subfolder = os.path.relpath(root, local_folder_path)

        # Construct the GCS folder path
        gcs_folder_path = f'{save_subfolder}/{run_date}'

        remove_gcs_folder_if_exists(bucket_name=bucket_name, folder_path=gcs_folder_path)


        for file in files:
            if file.endswith('.json'):
                local_file_path=os.path.join(root, file)
                # save_location = os.path.join(save_folder, save_subfolder)
                house_votes_json, sen_votes_json = get_votes_from_bill(bill_json=local_file_path, filter_date=filter_date)
                
                if house_votes_json: # Only upload if json is not empty
                    votes_json_filename = f'votes_{os.path.basename(local_file_path)}'
                    house_destination_blob_name = f'house_votes/{gcs_folder_path}/{votes_json_filename}'
                    
                    logging.info(f'Saving {votes_json_filename} to {house_destination_blob_name}')
                    upload_to_gcs_from_string(object=house_votes_json, bucket_name=bucket_name, destination_blob_name=house_destination_blob_name)

                if sen_votes_json:
                    votes_json_filename = f'votes_{os.path.basename(local_file_path)}'
                    sen_destination_blob_name = f'senate_votes/{gcs_folder_path}/{votes_json_filename}'
                    
                    logging.info(f'Saving {votes_json_filename} to {sen_destination_blob_name}')
                    upload_to_gcs_from_string(object=sen_votes_json, bucket_name=bucket_name, destination_blob_name=sen_destination_blob_name)


def get_votes_from_bill(bill_json, filter_date):

    """
    Takes votes specified in a bill json (downloaded from Congress.gov) and returns jsons of votes from indicated url.
    URLs from votes are from US Senate and House of Representative websites.

    Args:
        bill_json (json):       Bill record that contains information on recorded votes and link to vote xml
        filter_date (datetime): Used as cutoff for votes returned. Function will only return votes after 'filter_date'.

    Returns:
        tup: Tuple of newline-delimited json strings containing house and senate votes respectively
    """

    votes_list = []
    
    logging.info(f'Opening {bill_json}')

    with open(bill_json, encoding='latin-1') as f:
        for line in f:
            json_content = json.loads(line)
            bill = dict(json_content)

            bill_number = bill.get('number', bill.get('billNumber', None))
            bill_type = bill['type']
            logging.info(f"Opened bill {bill_type} {bill_number}")

            # Get actions from bill that contain RecordedVotes item
            bill_actions = [action for action in bill['actions']['item'] if action.get('recordedVotes', {})]
            recorded_votes = [action.get('recordedVotes', {}).get('recordedVote',{}) for action in bill_actions]
            
            # Filter for new votes based on date of last vote in existing data
            recorded_votes_incremental = list( \
                                                filter(lambda x: datetime.strptime(x.get('date', '1900-01-01T00:00:00Z'), '%Y-%m-%dT%H:%M:%SZ') > filter_date, 
                                                       recorded_votes))

            # return recorded_votes
            for vote in recorded_votes_incremental:
                if vote:
                    logging.info(f'Extracting votes from {bill_type} {bill_number}')
                    try:
                        vote_num = str(vote['rollNumber']).zfill(0)
                        if vote['chamber'] == 'Senate':
                            # xml_path = vote['url']
                            # response = requests.get(xml_path)
                            chamber = 'Senate'
                        else:
                            vote_year = pd.to_datetime(vote['date']).year
                            # Add leading 0 for roll calls in the single or double digits
                            # xml_path = f'https://clerk.house.gov/evs/{vote_year}/roll{vote_num}.xml'
                            chamber = 'House of Representatives'
                        
                        xml_path = vote.get('url', None)
                        if xml_path:
                            response = requests.get(xml_path)
                        else:
                            logging.info(f'No url for vote number {vote_num} for {bill_type} {bill_number}')

                        if response.status_code == 200:
                            response_dict = xmltodict.parse(response.content)
                            response_dict = replace_special_characters(response_dict)
                            response_dict['chamber'] = chamber
                            response_dict['bill_number'] = bill_number
                            response_dict['bill_type'] = bill_type
                            votes_list.append(response_dict)
                        else:
                            logging.info(f'Vote number {vote_num} request failed for {bill_type} {bill_number}')                        
                        
                    except Exception as e:
                        print(f"An exception occurred", e, f"Failed to save one or more votes for {bill_type} {bill_number}")
                else:
                    logging.info(f"Opened bill {bill_type} {bill_number} has no votes at this time")
                    
    house_batch_json_str = "\n".join([json.dumps(obj) for obj in votes_list if obj['chamber'] == 'House of Representatives'])
    sen_batch_json_str = "\n".join([json.dumps(obj) for obj in votes_list if obj['chamber'] == 'Senate'])

    return (house_batch_json_str, sen_batch_json_str)