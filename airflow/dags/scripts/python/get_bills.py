import requests
import pandas
from dotenv import load_dotenv
import os
import json
from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd
from google.cloud import storage
import pyarrow.parquet as pq
import io
from io import BytesIO
import logging

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
    system_code: str
    name: str

@dataclass
class RecordedVote:
    roll_number: str
    url: str
    chamber: str
    congress: str
    date: str
    session_number: str

@dataclass
class ActionSchema:
    actionDate: str
    actionTime: Optional[str]
    text: str
    type: str
    action_code: Optional[str]
    source_system: Dict[int, str]
    committees: Optional[List[Committee]]
    recorded_votes: Optional[List[RecordedVote]]
    calendar: Optional[str]
    calendarNumber: Optional[str]

@dataclass
class TermsItem:
    chamber: str
    start_year: int
    end_year: int
    url: str

@dataclass
class MemberSchema:
    bioguideId: str
    state: str
    partyName: str
    district: int
    name: str
    terms: List[TermsItem]
    depiction: Dict[str, str]


@dataclass
class PaginationInfo:
    count: int
    next: str
    prev: Optional[str]=None

@dataclass
class RequestInfo:
    contentType: str
    format: str

@dataclass
class ApiResponse:
    bills: Optional[List[BillSchema]]
    action: Optional[List[ActionSchema]]
    members: Optional[List[MemberSchema]]
    pagination: PaginationInfo
    request: RequestInfo

    def __post_init__(self):
        self.pagination = PaginationInfo(**self.pagination)
        self.request = RequestInfo(**self.request)
        self.bills = [BillSchema(**bill) for bill in self.bills]



def get_and_upload_data(base_url, start_date, end_date, bucket, congress=None):

    load_dotenv()
    api_key = os.getenv('congress_api_key')
    
    page_count = get_page_count(base_url=base_url, 
                                start_date=start_date, 
                                end_date=end_date,
                                limit=250,
                                api_key=api_key)
    
    for i in range(0, page_count):
        
        offset=i*250
        
        bills = get_bills(base_url, 
                        start_date, 
                        end_date, 
                        limit=250, 
                        offset=offset, 
                        api_key=api_key)
        
        file_name = f'bills/bills_{i*250+1}_{i*250+250}.parquet'
        upload_to_gcs_as_parquet(obj_list=bills,
                                 bucket_name='congress_data',
                                 file_name=file_name)
        print(f'{i*250+250} of {page_count} records uploaded')


def paginate(func, params):
    load_dotenv()
    api_key = os.getenv('congress_api_key')
    
    page_count = get_page_count(base_url=base_url, 
                                start_date=start_date, 
                                end_date=end_date,
                                limit=250,
                                api_key=api_key)
    
    for i in range(0, page_count):
        


def get_page_count(params, start_date, end_date, limit, api_key, data_type):
        
    start_date = params.get('start_date', None)
    end_date   = params.get('end_date', None)
    limit      = params.get('limit', None)
    congress   = params.get('congress', None)
    bill_type  = params.get('bill_type', None)
    bill_number  = params.get('bill_number', None)
    api_key    = params.get('api_key', None)

    if data_type == 'bills':
        path = f'https://api.congress.gov/v3/bill?format=json&fromDateTime={start_date}&toDateTime={end_date}&offset=0&limit={limit}&api_key={api_key}'
    
    elif data_type == 'member':
        path =f'https://api.congress.gov/v3/member?format=json&fromDateTime={start_date}&toDateTime={end_date}&offset=0&limit={limit}&api_key={api_key}'

    elif data_type == 'actions':
        path == f'https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}/actions'
    
    logging.info(f'executing API call for {path}')

    response = requests.get(path)
    
    api_response = ApiResponse(**response.json())
    
    pag_info = api_response.pagination
    
    page_count = pag_info.count
    
    return page_count

def get_bills(base_url, start_date, end_date, limit, offset, api_key):


    path = f'{base_url}?format=json&offset=0&fromDateTime={start_date}&toDateTime={end_date}&offset=0&limit={limit}&offset={offset}&api_key={api_key}'



    response = requests.get(path)

    api_response = ApiResponse(**response.json())
    
    request_info = api_response.request
    pagination_info = api_response.pagination
    bills = api_response.bills
    
    return bills


def upload_to_gcs_as_parquet(obj_list, bucket_name, file_name):
    # Convert BillSchema objects to a list of dictionaries
    data = [vars(obj) for obj in obj_list]
    
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
    