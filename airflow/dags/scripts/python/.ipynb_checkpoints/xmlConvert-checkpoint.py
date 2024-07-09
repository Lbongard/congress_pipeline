import os
import xmltodict
import json
import subprocess
import logging
from bs4 import BeautifulSoup
from datetime import datetime

# def convert_xml_to_newline_json(xml_content):
#     json_data = xmltodict.parse(xml_content)
#     newline_json = '\n'.join(json.dumps({key: value}) for key, value in json_data.items())
#     return newline_json

def convert_folder_xml_to_newline_json(folder):
    
    for subfolder in os.listdir(folder):
        subfolder_path = os.path.join(folder, subfolder)
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
                            # only keep needed fields
                            json_object_parsed = enforce_schema(json_data=json_object['billStatus'])
                            json_objects.append(json_object_parsed)
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
                output_file = f'{subfolder_path}/{subfolder}bill_status_{batch_count}.json'
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
                

def check_for_votes(json_input):
    if 'recordedVotes' in json.dumps(json_input):
        return True
    else:
        return False
    
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


def enforce_schema(json_data):
    conform_data = {
        "bill": {
            "number": json_data.get("bill", {}).get("number"),
            "updateDate": json_data.get("bill", {}).get("updateDate"),
            "type": json_data.get("bill", {}).get("type"),
            "introducedDate": json_data.get("bill", {}).get("introducedDate"),
            "congress": json_data.get("bill", {}).get("congress"),
            "committees": {
                "item": [item_dict(item) for item in ensure_list(get_item_if_exists(json_data.get("bill", {}).get("committees", {})))]
            },
            "actions": {
                "item": [action_dict(action) for action in ensure_list(get_item_if_exists(json_data.get("bill", {}).get("actions", {})))]
            },
            "sponsors": {
                "item": [sponsor_dict(sponsor) for sponsor in ensure_list(get_item_if_exists(json_data.get("bill", {}).get("sponsors", {})))]
            },
            "cosponsors": {
                "count": json_data.get("bill", {}).get("cosponsors", {}).get("count"),
                "item": [cosponsor_dict(cosponsor) for cosponsor in ensure_list(get_item_if_exists(json_data.get("bill", {}).get("cosponsors", {})))]
            },
            "policyArea": {
                "name": json_data.get("bill", {}).get("policyArea", {}).get("name")
            },
            "subjects": {
                "legislativeSubjects": {
                    "item": [subject_dict(subject) for subject in ensure_list(get_item_if_exists(json_data.get("bill", {}).get("subjects", {}).get("legislativeSubjects", {})))]
                }
            },
            "title": json_data.get("bill", {}).get("title"),
            "latestAction": {
                "actionDate": json_data.get("bill", {}).get("latestAction", {}).get("actionDate"),
                "text": json_data.get("bill", {}).get("latestAction", {}).get("text")
            },
            "most_recent_text": get_most_recent_text(json_data.get("bill", {}).get('summaries', {}).get('summary', {}))
        }
    }

    return conform_data


def item_dict(item):
    if isinstance(item, dict):
        return {
            "name": item.get("name"),
            "chamber": item.get("chamber"),
            "type": item.get("type")
        }
    return {}


def action_dict(action):
    if isinstance(action, dict):
        return {
            "actionDate": action.get("actionDate"),
            "text": action.get("text"),
            "type": action.get("type"),
            "actionCode": action.get("actionCode"),
            "recordedVotes": [vote_dict(vote) for vote in ensure_list(action.get("recordedVotes", {}).get("recordedVote", []))]
        }
    return {}


def sponsor_dict(sponsor):
    if isinstance(sponsor, dict):
        return {
            "bioguideID": sponsor.get("bioguideID"),
            "fulName": sponsor.get("fulName"),
            "firstName": sponsor.get("firstName"),
            "lastName": sponsor.get("lastName")
        }
    return {}


def cosponsor_dict(cosponsor):
    if isinstance(cosponsor, dict):
        return {
            "bioguideID": cosponsor.get("bioguideID")
        }
    return {}


def subject_dict(subject):
    if isinstance(subject, dict):
        return {
            "name": subject.get("name")
        }
    return {}


def vote_dict(vote):
    if isinstance(vote, dict):
        return {
            "rollNumber": vote.get("rollNumber"),
            "url": vote.get("url"),
            "chamber": vote.get("chamber"),
            "congress": vote.get("congress"),
            "date": vote.get("date"),
            "sessionNumber": vote.get("sessionNumber")
        }
    return {}

def get_most_recent_text(item):
    summary_list = ensure_list(item)
    filtered_summary_list = [summary for summary in summary_list if 'text' in summary]

    if not filtered_summary_list:
        return None
    
    most_recent_summary = max(filtered_summary_list, key=lambda x: datetime.strptime(x['actionDate'], '%Y-%m-%d'))

    most_recent_text = most_recent_summary['text'][:1000]

    formatted_most_recent_text = BeautifulSoup(most_recent_text, 'html.parser').prettify()
    
    return formatted_most_recent_text

def ensure_list(item):
    if isinstance(item, list):
        return item
    elif item:
        return [item]
    return []

def get_item_if_exists(input_data):
    if input_data:
        return input_data.get("item", [])
    else:
        return []