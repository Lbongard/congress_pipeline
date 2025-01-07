import os
import xmltodict
import json
from datetime import datetime
from .utils import *


def convert_folder_xml_to_newline_json(folder, project_id, dataset_id, table_id, default_date='2015-01-01T00:00:00Z', filter_files=True):
    """
    Converts XML files in a folder to newline-separated JSON files, filtering based on a date field for incremental processing.

    Args:
        folder (str):                  Path to the folder containing XML files to be converted.
        project_id (str):              Google Cloud Project ID.
        dataset_id (str):              BigQuery dataset ID for fetching the maximum update date.
        table_id (str):                BigQuery table ID for fetching the maximum update date.
        default_date (str, optional):  Default date to use if no data is found in the BigQuery table. Defaults to '2015-01-01T00:00:00Z'.
        filter_files (bool, optional): Whether to filter XML files based on the `updateDateIncludingText` field. Defaults to True.

    Returns:
        None
    """
    
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
                            
                            if json_object['billStatus']['bill'].get('updateDateIncludingText', None):
                                bill_update_date = datetime.strptime(json_object['billStatus']['bill']['updateDateIncludingText'], '%Y-%m-%dT%H:%M:%SZ')
                            else: # Some bills don't have UpdateDateIncludingText field
                                bill_update_date = datetime.strptime(json_object['billStatus']['bill']['updateDate'], '%Y-%m-%dT%H:%M:%SZ')

                            if bill_update_date >= filter_date:
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