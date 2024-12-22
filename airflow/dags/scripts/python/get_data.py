import requests
import pandas
from dotenv import load_dotenv
import os
import json
import dataclasses
from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd
from google.cloud import storage, bigquery
import pyarrow.parquet as pq
import io
from io import BytesIO
import logging
import itertools
import xmltodict
from airflow.operators.python import PythonOperator
from airflow import DAG
import subprocess
import shutil
from datetime import datetime


def convert_folder_xml_to_newline_json(folder, project_id, dataset_id, table_id, default_date='2015-01-01T00:00:00Z', filter_files=True):
    
    # Get max updated date from BigQuery for filtering API results for incremental load
    filter_date = get_max_updated_date(project_id=project_id, 
                                        dataset_id=dataset_id, 
                                        table_id=table_id, 
                                        default_date=default_date, 
                                        date_field='updateDateIncludingText')
                                                    

    # Loop through downloaded xml files
    for x in os.walk(folder):
        subdir = x[0]
        subfolder_path = os.path.join(folder, subdir)
        if os.path.isdir(subfolder_path):
            files = os.listdir(subfolder_path)

            json_objects = []
            xml_files = []

            for filename in files:
                if filename.endswith(".xml"):
                    xml_file = os.path.join(subfolder_path, filename)
                    try:
                        with open(xml_file, "r") as f:
                            xml_content = f.read()
                            json_object = xmltodict.parse(xml_content)
                            # # only keep needed fields
                            # json_object_parsed = enforce_schema(json_data=json_object['billStatus'])
                            # json_objects.append(json_object_parsed)
                            bill_update_date = datetime.strptime(json_object['billStatus']['bill']['updateDateIncludingText'], '%Y-%m-%dT%H:%M:%SZ')
                            if bill_update_date > filter_date:
                                bill_object = json_object['billStatus']['bill']
                                bill_object_conformed = ensure_item_is_list(bill_object)
                                json_objects.append(bill_object_conformed)
                            xml_files.append(xml_file)
                    except:
                        # logging.info(f'Failed to convert data for {xml_file}')
                        raise Exception(f'failed to convert data for {xml_file}')
                    
            batch_size = 250
            batch_count = 0

            for i in range(0, len(json_objects), batch_size):
                # Get the current batch
                batch = json_objects[i:i + batch_size]

                # Convert batch to newline-separated JSON
                batch_json_str = "\n".join([json.dumps(obj) for obj in batch])

                # Output the batch to a new file
                output_file = f'{subfolder_path}/bill_status_{batch_count}.json'
                with open(output_file, 'w') as f:
                    f.write(batch_json_str)
                
                # Increment the batch count
                batch_count += 1

            # Remove original xml files
            for xml_file in xml_files:
                if os.path.exists(xml_file):
                    try:
                        os.remove(xml_file)
                    except Exception as e:
                        print(f"Error deleting {xml_file}")
                else:
                    print(f'{xml_file} not found.')




def get_votes_for_saved_bills(local_folder_path, bucket_name, project_id, dataset_id, table_id, default_date='2015-01-01', **kwargs):

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
                house_votes_json, sen_votes_json = get_votes(bill_json=local_file_path, filter_date=filter_date)
                
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




def get_votes(bill_json, filter_date):

    votes_list = []
    
    logging.info(f'Opening {bill_json}')

    with open(bill_json, encoding='latin-1') as f:
        for line in f:
            json_content = json.loads(line)
            bill = dict(json_content)

            bill_number = bill['number']
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

    # save_dir = os.path.join(save_location, bill_type)
    
    # if not os.path.exists(save_location):
    #     os.makedirs(save_location)

    return (house_batch_json_str, sen_batch_json_str)
    
    # with open(file_name, "w") as outfile:
    #     outfile.write(batch_json_str)

               


def get_members(params, project_id, dataset_id, table_id, bucket_name, **kwargs):

    max_updated_date = get_max_updated_date(project_id=project_id, dataset_id=dataset_id, table_id=table_id, date_field='updateDate')

    # Get run date to name subfolder in gcs for this run
    ti = kwargs['ti']
    run_date = ti.execution_date.date()

    members_subfolder_name = f'members/{run_date}'
    remove_gcs_folder_if_exists(bucket_name=bucket_name, folder_path=members_subfolder_name)

    
    def get_members_generator(params, max_updated_date):

        members_to_upload = []
            
        members_list = get_members_list_api_call(params)['members']

        for member in members_list:
            
            if datetime.strptime(member['updateDate'], '%Y-%m-%dT%H:%M:%SZ') > max_updated_date:
                bioguideID = member['bioguideId']
                terms_all = member['terms']
                
                for term in terms_all['item']:
                    # Get end of each term to determine if member served in desired time frame
                    term_end_year = term.get('endYear', None)

                    # If member is still serving (no term end date) or term ended within desired time frame, get full member data
                    if (term_end_year is None) or (term_end_year > end_year_limit):

                        r = requests.get(f'https://api.congress.gov/v3/member/{bioguideID}/?format=json&api_key={api_key}')
                        
                        member_json = r.json()['member']
                        # Add summarized terms list
                        member_json['terms_all'] = terms_all
                        
                        members_to_upload.append(member_json)

                        break

        return members_to_upload

        
    # Set parameters to start looping through API
    api_key = params.get('api_key', None)
    limit = params.get('limit', None)
    max_records = params.get('max_records', None)
    end_year_limit = params.get('end_year_limit', None)
    logging.info(f'Max records passed as {max_records}')
    
    record_count = get_member_record_count(params=params)
    records_to_fetch = max_records if max_records else record_count
    # logging.info(f'About to start fetching {records_to_fetch} records')

    
    for i in range(0, records_to_fetch, limit):

        params['offset'] = i
        
        members_to_upload = get_members_generator(params=params, max_updated_date=max_updated_date)

        ndjson_data = '\n'.join(json.dumps(member) for member in members_to_upload)


        destination_blob_name= f'{members_subfolder_name}/members_{i}_{i + limit}.json'

        upload_to_gcs_from_string(object=ndjson_data, bucket_name=bucket_name, destination_blob_name=destination_blob_name)



def get_member_record_count(params):
        
    api_response = get_members_list_api_call(params)
    
    pag_info = api_response['pagination']
    
    record_count = pag_info['count']

    return record_count


def get_members_list_api_call(params):
        
    start_date  = params.get('start_date', None)
    end_date    = params.get('end_date', None)
    limit       = params.get('limit', None)
    api_key     = params.get('api_key', None)
    offset      = params.get('offset', None)

    path = f'https://api.congress.gov/v3/member?format=json&fromDateTime={start_date}&toDateTime={end_date}&offset={offset}&limit={limit}&api_key={api_key}'
    
    # logging.info(f'executing API call for {path}')

    response = requests.get(path)

    member_list = response.content

    member_list_json = json.loads(member_list)

    return member_list_json


def upload_to_gcs_from_string(object, bucket_name, destination_blob_name):
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    # Upload the contents of the buffer to GCS
    blob.upload_from_string(object)

    logging.info(f'File {destination_blob_name} uploaded to {bucket_name}.')



def upload_to_gcs_as_parquet(obj_list, bucket_name, file_name, data_type):
    
    # Object containing list of members in API response is plural. Why 'member' in endpoint is not also plural I could not tell you.
    if data_type == 'member':
        data_type = 'members'

    # Convert Schema objects to a list of dictionaries
    if data_type in ['bills', 'actions', 'members']:
        data = [vars(obj) for obj in getattr(obj_list, data_type)]
    else:
        data = obj_list
    
    # Convert list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(data)
    
    # Save DataFrame to Parquet file in memory
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)  # Reset buffer position to the beginning
    
    # Upload Parquet file buffer to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
    print(f"Parquet data uploaded to gs://{bucket_name}/{file_name}")


def upload_folder_to_gcs(local_folder_path, bucket_name, destination_folder, run_date_subfolder=True, remove_local=True, **kwargs):
    
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
                print(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_blob_name}")
                if remove_local:
                    os.remove(local_file_path)
            except Exception as e:
                logging.info(f"Failed to upload {local_file_path} to gs://{bucket_name}/{gcs_blob_name}")
            


def conform_votes(vote_data, bill_type, bill_number, roll_call_number, chamber):
    conform_data = {
        'vote': get_vote_metadata(data=vote_data, chamber=chamber, bill_type=bill_type, bill_number=bill_number),
        'legislator': [legislator_dict(item, chamber) for item in get_raw_votes(vote_data, chamber=chamber)]
    }
    
    return conform_data

def get_vote_metadata(data, chamber, bill_type, bill_number):
    
    if chamber == 'House':
        vote_date = data['rollcall-vote']['vote-metadata']['action-date']
        date_obj = datetime.strptime(vote_date, '%d-%b-%Y')
        formatted_vote_date = date_obj.strftime('%Y-%m-%d')
        congress = data['rollcall-vote']['vote-metadata']['congress']
        session = data['rollcall-vote']['vote-metadata']['session']
        
        
        return {'date': formatted_vote_date,
                'congress': congress,
                'session': session,
                'bill_type': bill_type,
                'bill_number': bill_number,
                'chamber': 'House',
                'roll_call_number': data['rollcall-vote']['vote-metadata']['rollcall-num'],
                'result': data['rollcall-vote']['vote-metadata']['vote-result'],
                'totals': {'yea': data['rollcall-vote']['vote-metadata']['vote-totals']['totals-by-vote']['yea-total'],
                           'nay': data['rollcall-vote']['vote-metadata']['vote-totals']['totals-by-vote']['nay-total'],
                           'present': data['rollcall-vote']['vote-metadata']['vote-totals']['totals-by-vote']['present-total'],
                           'not_voting': data['rollcall-vote']['vote-metadata']['vote-totals']['totals-by-vote']['not-voting-total']
                          }
                }
        
    elif chamber == 'Senate':
        vote_date = data['roll_call_vote']['vote_date']
        vote_date = vote_date.replace(': ', ':')
        date_obj = datetime.strptime(vote_date, '%B %d, %Y, %I:%M %p')
        formatted_vote_date = date_obj.strftime('%Y-%m-%d')
        congress = data['roll_call_vote']['congress']
        session = data['roll_call_vote']['session']
        
        
        return {'date': formatted_vote_date,
                'congress': congress,
                'session': session,
                'bill_type': bill_type,
                'bill_number': bill_number,
                'chamber': 'Senate',
                'roll_call_number': data['roll_call_vote']['vote_number'],
                'result': data['roll_call_vote']['vote_result'],
                'totals': {'yea': data['roll_call_vote']['count']['yeas'],
                           'nay': data['roll_call_vote']['count']['nays'],
                           'present': data['roll_call_vote']['count']['present'],
                           'not_voting': data['roll_call_vote']['count']['absent']}
               }
                
   
    else:
        return {}
    
def get_raw_votes(data, chamber):
    if chamber == 'House':
        return data['rollcall-vote']['vote-data']['recorded-vote']
    if chamber == 'Senate':
        return data['roll_call_vote']['members']['member']
    return {}
        

def legislator_dict(item, chamber):
        if chamber == 'House':
            if isinstance(item, dict):
                return {
                    'id': item.get("legislator").get("@name-id"),
                    'first_name': None,
                    'last_name': item.get("legislator").get("@unaccented-name"),
                    'party': item.get("legislator").get("@party"),
                    'state': item.get("legislator").get("@state"),
                    'vote' : item.get("vote")
                    
                }
            return {}
        
        if chamber == 'Senate':
            if isinstance(item, dict):
                return {
                    'id': item.get("lis_member_id"),
                    'first_name': item.get("first_name"),
                    'last_name': item.get("last_name"),
                    'party': item.get("party"),
                    'state': item.get("state"),
                    'vote' : item.get("vote_cast")
                }
            return {}


def get_senate_ids(save_folder, bucket_name):

    if os.path.exists(save_folder):
        shutil.rmtree(save_folder)
    os.makedirs(save_folder)
    
    response = requests.get('https://www.senate.gov/about/senator-lookup.xml')

    response.raise_for_status()
    
    sen_dict = xmltodict.parse(response.content)
    sen_id_list = [replace_special_characters(sen) for sen in sen_dict['senators']['senator']]

    for sen in sen_id_list:
        if isinstance(sen.get('lisid', ''), list):
            pass
        else:
            sen['lisid'] = [sen.get('lisid', '')]

    sen_id_json = "\n".join([json.dumps(sen) for sen in sen_id_list])

    remove_gcs_folder_if_exists(bucket_name, save_folder)

    upload_to_gcs_from_string(object=sen_id_json, bucket_name=bucket_name, destination_blob_name=f'{save_folder}/senate_ids.json')





def conform_senate_ids(data):
    
    
    sen_dict = xmltodict.parse(data)
    sen_id_list = [sen_id_dict(sen) for sen in sen_dict['senators']['senator']]
    
    return sen_id_list
        
def sen_id_dict(item):
    if isinstance(item, dict):
        
        if isinstance(item.get('lisid'), list):
            lisid = item.get('lisid')[0]
        else: 
            lisid = item.get('lisid')

        return {
            'first_name': item.get('full_name').get('first_name'),
            'last_name': item.get('full_name').get('last_name'),
            'state': item.get('state'),
            'bioguideID': item.get('bioguide'),
            'lisid':lisid
        }
    return {}

def upload_to_gcs_from_string(object, bucket_name, destination_blob_name):
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    # Upload the contents of the buffer to GCS
    blob.upload_from_string(object)

    logging.info(f'File {destination_blob_name} uploaded to {bucket_name}.')


def get_max_updated_date(date_field, project_id, dataset_id, table_id, default_date='2015-01-01T00:00:00Z', format='%Y-%m-%dT%H:%M:%SZ'):

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


