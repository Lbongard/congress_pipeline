import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

def conform_data(json_data):
    conform_data = {
        "bill": {
            "number": json_data.get("bill", {}).get("number"),
            "updateDate": json_data.get("bill", {}).get("updateDate"),
            "type": json_data.get("bill", {}).get("type"),
            "introducedDate": json_data.get("bill", {}).get("introducedDate"),
            "congress": json_data.get("bill", {}).get("congress"),
            "committees": {
                "item": [item_dict(item) for item in ensure_list(json_data.get("bill", {}).get("committees", {}).get("item", []))]
            },
            "actions": {
                "item": [action_dict(action) for action in ensure_list(json_data.get("bill", {}).get("actions", {}).get("item", []))]
            },
            "sponsors": {
                "item": [sponsor_dict(sponsor) for sponsor in ensure_list(json_data.get("bill", {}).get("sponsors", {}).get("item", []))]
            },
            "cosponsors": {
                "count": json_data.get("bill", {}).get("cosponsors", {}).get("count"),
                "item": [cosponsor_dict(cosponsor) for cosponsor in ensure_list(json_data.get("bill", {}).get("cosponsors", {}).get("item", []))]
            },
            "policyArea": {
                "name": json_data.get("bill", {}).get("policyArea", {}).get("name")
            },
            "subjects": {
                "legislativeSubjects": {
                    "item": [subject_dict(subject) for subject in ensure_list(json_data.get("bill", {}).get("subjects", {}).get("legislativeSubjects", {}).get("item", []))]
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