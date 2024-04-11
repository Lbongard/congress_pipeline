import os
import xmltodict
import json
import subprocess
import logging

def convert_xml_to_newline_json(xml_content):
    json_data = xmltodict.parse(xml_content)
    newline_json = '\n'.join(json.dumps({key: value}) for key, value in json_data.items())
    return newline_json

def convert_folder_xml_to_newline_json(folder):
    for root, dirs, files in os.walk(folder):
        for filename in files:
            if filename.endswith(".xml"):
                xml_file = os.path.join(root, filename)
                try:
                    with open(xml_file, "r") as f:
                        xml_content = f.read()
                    # newline_json_data = convert_xml_to_newline_json(xml_content)

                    # Only keep jsons with recorded votes
                    if 'recordedVotes' in str(xml_content):
                        json_data = xmltodict.parse(xml_content)

                        # only keep needed fields
                        json_data = enforce_schema(json_data=json_data['billStatus'])

                        json_data = replace_special_characters(json_data)
                        newline_json_data = '\n'.join(json.dumps({key: value}) for key, value in json_data.items())
                        # newline_json_data = convert_xml_to_newline_json(xml_content)
                        json_filename = os.path.splitext(filename)[0] + ".json"
                        json_file = os.path.join(root, json_filename)
                        # Replace special characters in keys
                        with open(json_file, "w") as f:
                            f.write(newline_json_data)
                            # json.dump(newline_json_data, f)
                            print(f"Converted {xml_file} to {json_file}")
                except:
                    # logging.info(f'Failed to convert data for {xml_file}')
                    raise Exception(f'failed to convert data for {xml_file}')
                    

                os.remove(xml_file)
                

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
            }
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