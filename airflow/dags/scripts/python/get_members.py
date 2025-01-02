import requests
import os
import json
import logging
import xmltodict
import shutil
from datetime import datetime
from .utils import *


def get_members(params, project_id, dataset_id, table_id, bucket_name, **kwargs):

    """
    Gets members data from Congress.gov API and uploads data to GCS. 
    Only fetches records updated after most recent UpdatedDate in specified BigQuery table

    Args:
        params (dict):     Parameters used in Congress API call
        project_id (str):  GCP Project ID for GCS upload
        dataset_id (str):  GCP Dataset ID for GCS upload
        table_id (str):    GCP table ID for to check UpdatedDate for limiting query
        bucket_name (str): GCS bucket for uploading records

    Returns:
        None
    """

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
    """
    Gets number of pages returned by an API call. Used in pagination function

    Args:
        params (dict):     Parameters used in Congress API call
        
    Returns:
        int: Number of pages in API call
    """
        
    api_response = get_members_list_api_call(params)
    
    pag_info = api_response['pagination']
    
    record_count = pag_info['count']

    return record_count


def get_members_list_api_call(params):

    """
    Gets list of members returned by API call

    Args:
        params (dict): Parameters used in Congress API call
        
    Returns:
        json: Members list
    """
        
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

def get_senate_ids(save_folder, bucket_name):
    
    """
    Gets list of historical senate IDs and uploads to GCS
    Args:
        save_folder (str): Subfolder in GCS bucket in which to save results
        bucket_name (str): GCS bucket for uploading records
        
    Returns:
        None
    """

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
