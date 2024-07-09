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

# https://medium.com/@danilo.drobac/real-world-python-for-data-engineering-1-api-pagination-6b92fec2fc50


@dataclass
class TermsItem:
    chamber: str
    start_year: int
    end_year: Optional[int]=None

@dataclass
class DepictionItem:
    attribution: Optional[str]=None
    imageUrl: Optional[str]=None

@dataclass
class MemberSchema:
    bioguideId: str
    state: str
    partyName: str
    name: str
    terms: List[TermsItem]
    updateDate: str
    url: str
    district: Optional[int]=None
    depiction: Optional[Dict[str, str]]=None

@dataclass
class PaginationInfo:
    count: int
    next: Optional[str]=None
    prev: Optional[str]=None

@dataclass
class RequestInfo:
    format: str
    billNumber: Optional[str]=None
    billType: Optional[str]=None
    billUrl: Optional[str]=None
    congress: Optional[str]=None
    contentType: Optional[str]=None

@dataclass
class ApiResponse:
    pagination: PaginationInfo
    request: RequestInfo
    members: Optional[List[MemberSchema]]=None

    def __post_init__(self):
        self.pagination = PaginationInfo(**self.pagination)
        self.request = RequestInfo(**self.request)
        self.members = [MemberSchema(**member) for member in self.members]

def get_votes_for_saved_bills(local_folder_path, save_folder):
    # Iterate over files in the local folder
    if os.path.exists(save_folder):
        shutil.rmtree(save_folder)
    os.makedirs(save_folder)

    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            get_votes(bill_json=local_file_path, save_folder=save_folder)


def get_votes(bill_json, save_folder):
    
    votes_list = []

    with open(bill_json) as f:
        for line in f:
            json_content = json.loads(line)
            bill = dict(json_content)

            bill_number = bill['bill']['number']
            bill_type = bill['bill']['type']
            logging.info(f"Opened bill {bill_type} {bill_number}")

            bill_actions = [action for action in bill['bill']['actions']['item']]
            recorded_votes = [vote for action in bill_actions for vote in action['recordedVotes']]
    
            # return recorded_votes
            for vote in recorded_votes:
                try:
                    vote_num = str(vote['rollNumber']).zfill(0)
                    if vote['chamber'] == 'Senate':
                        
                        xml_path = vote['url']
                        response = requests.get(xml_path)

                        response_dict = xmltodict.parse(response.content)
                        # votes_data = response_dict['roll_call_vote']['members']['member']

                    else:
                        vote_year = pd.to_datetime(vote['date']).year
                        # Add leading 0 for roll calls in the single or double digits
                        xml_path = f'https://clerk.house.gov/evs/{vote_year}/roll{vote_num}.xml'
                        response = requests.get(xml_path)

                        # Turning Response into a json
                        response_dict = xmltodict.parse(response.content)
                        # votes_data = response_dict['rollcall-vote']['vote-data']['recorded-vote']

                    votes_data = conform_votes(vote_data=response_dict, 
                                                bill_type=bill_type, 
                                                bill_number=bill_number, 
                                                roll_call_number=vote_num, 
                                                chamber=vote['chamber'])
                    votes_list.append(votes_data)
                    
                except Exception as e:
                    print(f"An exception occurred", e, "Failed to save one or more votes for {bill_type} {bill_number}")
                    
    batch_json_str = "\n".join([json.dumps(obj) for obj in votes_list])

    save_dir = os.path.join(save_folder, bill_type)
    
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    file_name = f'{save_dir}/votes_{os.path.basename(bill_json)}'
    
    with open(file_name, "w") as outfile:
        outfile.write(batch_json_str)

               


def get_members(params, save_folder):

    load_dotenv()
    api_key = os.getenv('congress_api_key')
    if os.path.exists(save_folder):
            shutil.rmtree(save_folder)
    os.makedirs(save_folder)

    limit = params.get('limit', None)
    max_records = params.get('max_records', None)
    logging.info(f'Max records passed as {max_records}')
    
    if not limit:
        logging.error('No record limit provided for pagination')
    
    record_count = get_member_record_count(params=params)
    records_to_fetch = max_records if max_records else record_count
    logging.info(f'About to start fetching {records_to_fetch} records')
    
    for i in range(0, records_to_fetch, limit):
        
        params['offset'] = i
        
        data = get_members_api_call(params)

        members_df = pd.DataFrame(data.members)

        filename = f"{save_folder}/members_{i}_{i + limit}.json"
        f = open(filename, "w")

        for row in members_df.index:
            f.write(members_df.loc[row].to_json() + "\n")

        print(f'{i+len(data.members)} of {records_to_fetch} member records saved')

def get_member_record_count(params):
        
    api_response = get_members_api_call(params)
    
    pag_info = api_response.pagination
    
    record_count = pag_info.count

    return record_count

def get_members_api_call(params):
        
    start_date  = params.get('start_date', None)
    end_date    = params.get('end_date', None)
    limit       = params.get('limit', None)
    api_key     = params.get('api_key', None)
    offset      = params.get('offset', None)

    path = f'https://api.congress.gov/v3/member?format=json&fromDateTime={start_date}&toDateTime={end_date}&offset={offset}&limit={limit}&api_key={api_key}'
    
    logging.info(f'executing API call for {path}')

    response = requests.get(path)

    api_response = ApiResponse(**response.json())

    return api_response



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


def upload_folder_to_gcs(local_folder_path, bucket_name, destination_folder):
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

            blob.upload_from_filename(local_file_path)

            print(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_blob_name}")


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
        
        return {'date': formatted_vote_date,
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
        
        return {'date': formatted_vote_date,
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


def get_senate_ids(save_folder):

    if os.path.exists(save_folder):
        shutil.rmtree(save_folder)
    os.makedirs(save_folder)
    
    response = requests.get('https://www.senate.gov/about/senator-lookup.xml')
    sen_id_list = conform_senate_ids(response.content)
    
    sen_id_df = pd.DataFrame(sen_id_list)

    filename = f"{save_folder}/senate_id_lookup.json"
    f = open(filename, "w")

    for row in sen_id_df.index:
        f.write(sen_id_df.loc[row].to_json() + "\n")



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