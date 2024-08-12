import redis
import pandas as pd
from google.cloud import bigquery
import pickle
import os
import json

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

config_path = os.path.abspath(os.getenv('TF_VAR_google_credentials'))

with open(config_path, 'r') as config_file:
    config = json.load(config_file)


def get_bigquery_client():
    return bigquery.Client()

def fetch_data_from_bigquery(client, query):
    query_job = client.query(query)
    results = query_job.result()
    data = results.to_dataframe()
    return data

def main():
    client = get_bigquery_client()
    query = """
    -- HOUSE MEMBERS QUERY
    SELECT  mems.name 
            ,mems.state 
            ,mems.bioguideID
            ,mems.imageURL
            ,mems.partyName
    FROM `Congress.dim_members` mems JOIN `Congress.dim_congressional_districts` dists
        ON (COALESCE(mems.district, 0) = dists.congressional_district) and (mems.state = dists.state)
    WHERE mems.bioguideID in (SELECT distinct bioguideID from `Congress.fact_roll_call_vote`)

    UNION DISTINCT

    -- SENATE MEMBERS QUERY
    SELECT mems.name 
           ,mems.state 
           ,mems.bioguideID
           ,mems.imageURL
           ,mems.partyName
    FROM `Congress.dim_members` mems JOIN `Congress.dim_congressional_districts` dists
        ON (mems.state = dists.state)
    WHERE mems.lisid in (SELECT distinct lisid from `Congress.fact_roll_call_vote`)
    """
    data = fetch_data_from_bigquery(client, query)
    
    # Serialize the dataframe and store in Redis
    serialized_data = pickle.dumps(data)
    redis_client.set('cached_data', serialized_data)

if __name__ == "__main__":
    main()
