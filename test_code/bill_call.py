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
    bills: List[BillSchema]
    pagination: PaginationInfo
    request: RequestInfo

    def __post_init__(self):
        self.pagination = PaginationInfo(**self.pagination)
        self.request = RequestInfo(**self.request)
        self.bills = [BillSchema(**bill) for bill in self.bills]

def get_and_upload_bills(base_url, start_date, end_date):

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
        upload_to_gcs_as_parquet(bills=bills,
                                 bucket_name='congress_data',
                                 file_name=file_name)
        print(f'{i*250+250} of {page_count} records uploaded')

def get_page_count(base_url, start_date, end_date, limit, api_key):
    
    path = f'{base_url}?format=json&fromDateTime={start_date}&toDateTime={end_date}&offset=0&limit={limit}&api_key={api_key}'
    
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


def upload_to_gcs_as_parquet(bills, bucket_name, file_name):
    # Convert BillSchema objects to a list of dictionaries
    data = [vars(obj) for obj in bills]
    
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
    
# upload_to_gcs_as_parquet(bills=bills, bucket_name='congress_data', file_name='bills/bills_0_250.parquet')

if __name__ == '__main__':
    
    start_date = '2023-03-13T00:00:00Z'
    end_date = '2024-03-14T00:00:00Z'

    get_and_upload_bills(base_url = 'https://api.congress.gov/v3/bill',
                         start_date=start_date,
                         end_date=end_date)