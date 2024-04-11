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
import traceback
import datetime.datetime

# https://medium.com/@danilo.drobac/real-world-python-for-data-engineering-1-api-pagination-6b92fec2fc50

@dataclass
class BillSchema:
    congress: int
    latestAction: Dict[str, str] #Update to date for first item?
    number: str
    originChamber: str
    originChamberCode: str
    title: str
    type: str
    updateDate: str
    updateDateIncludingText: str
    url: str

@dataclass
class Committee:
    url: str
    systemCode: str
    name: str

@dataclass
class RecordedVote:
    roll_number: str
    url: str
    chamber: str
    congress: str
    date: str
    sessionNumber: str

@dataclass
class ActionSchema:
    actionDate: str
    text: str
    type: str
    sourceSystem: Dict[int, str]
    actionTime: Optional[str]=None
    actionCode: Optional[str]=None
    committees: Optional[List[Committee]]=None
    recordedVotes: Optional[List[RecordedVote]]=None
    calendar: Optional[str]=None
    calendarNumber: Optional[str]=None

@dataclass
class TermsItem:
    chamber: str
    start_year: int
    end_year: int

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
    bills: Optional[List[BillSchema]]=None
    actions: Optional[List[ActionSchema]]=None
    members: Optional[List[MemberSchema]]=None

    def __post_init__(self):
        self.pagination = PaginationInfo(**self.pagination)
        self.request = RequestInfo(**self.request)
        if self.bills:
            self.bills = [BillSchema(**bill) for bill in self.bills]
        if self.actions:
            self.actions = [ActionSchema(**action) for action in self.actions]
        if self.members:
            self.members = [MemberSchema(**member) for member in self.members]

def get_and_upload_votes(bills_list, params):
    for bill in bills_list:
        params['congress'] = bill[0]
        logging.info(f'Congress = {bill[0]}')
        params['bill_number'] = bill[1]
        params['bill_type'] = bill[2].lower()

        # get_and_upload_data(params=params, data_type='actions')

        # Get Votes

        api_response = get_api_response(params, 'actions')

        actions_df = pd.DataFrame(api_response.actions)
        # print(actions_df)
      
        roll_call_votes = actions_df[actions_df['recordedVotes'].isna() == False].recordedVotes
        # Some empty votes left in?
        if roll_call_votes.empty == False:
            try:
                # print(roll_call_votes)
                for roll_call in roll_call_votes:
                    # print(roll_call)
                    for vote in roll_call:
                        if vote['chamber'] == 'Senate':
                            xml_path = vote['url']
                            response = requests.get(xml_path)
                            
                            # Turning Response into a json
                            response_dict = xmltodict.parse(response.content)
                            response_dict = replace_special_characters(response_dict)
                            votes_data = response_dict['roll_call_vote']
                        else:
                            vote_year = pd.to_datetime(vote['date']).year
                            # Add leading 0 for roll calls in the single or double digits
                            vote_num = str(vote['rollNumber']).zfill(0)
                            xml_path = f'https://clerk.house.gov/evs/{vote_year}/roll{vote_num}.xml'
                            response = requests.get(xml_path)

                            # Turning Response into a json
                            response_dict = xmltodict.parse(response.content)
                            response_dict = replace_special_characters(response_dict)
                            # votes_metadata = response_dict['rollcall-vote']['vote-metadata']
                            votes_data = response_dict['rollcall_vote']

                    # print(type(votes_data))

                    file_name = f'votes/{vote["chamber"]}/{params["bill_type"]}_{params["bill_number"]}_{vote["rollNumber"]}.json'

                    client = storage.Client()
                    bucket = client.bucket('congress_data')
                    blob = bucket.blob(file_name)
                    blob.upload_from_string(data=json.dumps(votes_data),
                                            content_type='application/json')
                    

                    
                    # upload_to_gcs_as_parquet(obj_list=votes_data,
                    #                         bucket_name='congress_data',
                    #                         file_name=file_name,
                    #                         data_type='votes')
                    
                    print(f" data uploaded to gs://'congress_data/{file_name}")
            except Exception as e:
                logging.error(f'Actions not fully uploaded for bill {params["bill_type"]} {params["bill_number"]} in Congress {params["congress"]}')
                print(e)
                print(traceback.format_exc())



def get_and_upload_data(params, data_type):

    load_dotenv()
    api_key = os.getenv('congress_api_key')
    limit = params.get('limit', None)
    # max_records = params.get(max_records)
    
    if not limit:
        logging.error('No record limit provided for pagination')
    
    record_count = get_record_count(params=params, data_type=data_type)

    for i in range(0, record_count, limit):
        
        params['offset'] = i*limit
        
        data = get_api_response(params,
                                 data_type)
        
        file_name = f'{data_type}/{i*limit+1}_{i*limit+len(getattr(data, data_type))}_{data_type}.parquet'

        upload_to_gcs_as_parquet(obj_list=data,
                                 bucket_name='congress_data',
                                 file_name=file_name,
                                 data_type=data_type)
        
        print(f'{i*limit+len(getattr(data, data_type))} of {record_count} {data_type} records uploaded')

        # Get actions associated with bills
        # API response needs to be called separately for each bill
        if data_type == 'actions':
                
            actions_df = pd.DataFrame(data.actions).reset_index()
            
            # try:    
            #     for index, row in actions_df.iterrows():
            #         if row.recordedVotes.isna() == False:
            #             for vote in row.recordedVotes:
            #                 pass
            # except:
            #         pass
            # {'chamber': 'House', 'congress': 118, 'date': '2024-03-12T21:13:39Z', 'rollNumber': 84, 'sessionNumber': 1, 'url': 'https://clerk.house.gov/evs/2024/roll84.xml'}
                            

            #         record_count = get_record_count(params=actions_params, data_type='actions')

            #         for i in range(0, record_count, limit):
            #             actions_params['offset'] = i*limit
            #             data = get_api_response(actions_params, 'actions')

            #             file_name = f"actions/{actions_params['bill_type']}_{actions_params['bill_number']}_actions_{i*limit+1}_{i*limit+len(data.actions)}.parquet"

            #             upload_to_gcs_as_parquet(obj_list=data,
            #                      bucket_name='congress_data',
            #                      file_name=file_name,
            #                      data_type='actions')
            #             print(f"Actions uploaded for bill {actions_params['bill_type']} {actions_params['bill_number']}")
                        
            # except:
            #     logging.ERROR(f'Failed to upload all actions data for bills in file {file_name}')


def get_record_count(params, data_type):
        
    api_response = get_api_response(params, data_type)
    
    pag_info = api_response.pagination
    
    record_count = pag_info.count

    return record_count

def get_api_response(params, data_type):
        
    start_date  = params.get('start_date', None)
    end_date    = params.get('end_date', None)
    limit       = params.get('limit', None)
    congress    = params.get('congress', None)
    bill_type   = params.get('bill_type', None)
    bill_number = params.get('bill_number', None)
    api_key     = params.get('api_key', None)
    offset      = params.get('offset', None)

    if data_type == 'bills':
        path = f'https://api.congress.gov/v3/bill?format=json&fromDateTime={start_date}&toDateTime={end_date}&offset={offset}&limit={limit}&api_key={api_key}&congress={congress}'
    
    elif data_type == 'member':
        path = f'https://api.congress.gov/v3/member?format=json&fromDateTime={start_date}&toDateTime={end_date}&offset={offset}&limit={limit}&api_key={api_key}'

    elif data_type == 'actions':
        path = f'https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}/actions?offset={offset}&limit={limit}&api_key={api_key}'
    
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

def get_bills_from_bq():
    # Fetch all bills from bigquery
    client = bigquery.Client()
    query = 'SELECT DISTINCT congress, number, type FROM Congress.bills'
    query_job = client.query(query)
    results = query_job.result()

    return [list(row.values()) for row in results]

def replace_special_characters(data):
    if isinstance(data, dict):
        new_dict = {}
        for key, value in data.items():
            
            new_key = key.replace('-', '_').\
                replace('@', ''). \
                replace('#','')  
            
            new_dict[new_key] = replace_special_characters(value)  # Recursively process nested dictionaries
        return new_dict
    elif isinstance(data, list):
        return [replace_special_characters(item) for item in data]  # Recursively process items in lists
    else:
        return data 
    
def filter_json(input_path, output_path, fields_to_include):
    for filename in os.listdir(input_path):
        if filename.endswith(".json"):
            input_file = os.path.join(input_path, filename)
            output_file = os.path.join(output_path, filename)
            
            with open(input_file, "r") as f:
                json_data = json.load(f)
                
                # Filter JSON data to include only specified fields
                filtered_json = {field: json_data.get(field) for field in fields_to_include}
                
            # Save the filtered JSON data to the output file
            with open(output_file, "w") as f:
                json.dump(filtered_json, f)

if __name__ == '__main__':

    params = {
        'limit': 250,
        'offset': 0,
        'start_date': "2024-01-01T00:00:00Z",
        'end_date': "2024-01-31T00:00:00Z",
        # 'congress': 118,
        # 'bill_type': 'hr',
        # 'bill_number':6276,
        'api_key': "WNue8kCDCOIlewAsULgnN8j6SqSgAZjE2sYbPsBb"
    }
    bills = get_bills_from_bq()[:10]
    # print(bills)
    
    get_and_upload_votes(bills_list=bills, params=params)