# fetch_data.py

from google.cloud import bigquery
import pandas as pd
import os
from google.oauth2 import service_account

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('TF_VAR_google_credentials')

# credentials = service_account.Credentials.from_service_account_file(creds_path)

def fetch_data_from_bigquery(query, date_cols=[]):
    client = bigquery.Client()
    query_job = client.query(query)
    df = query_job.to_dataframe()

    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    return df



def save_data(df, file_path):
    df.to_parquet(file_path)

if __name__ == "__main__":
    
    bills_by_member_query = """
                            SELECT *
                            FROM Congress.vw_sponsored_bills_by_member
                            """
    bills_by_member_df = fetch_data_from_bigquery(bills_by_member_query, date_cols=['introducedDate'])
    save_data(bills_by_member_df, "streamlit_data/sponsored_bills_by_member.parquet")

    votes_by_member_query = """SELECT *
                                FROM Congress.votes_by_member"""
    votes_by_member_df = fetch_data_from_bigquery(votes_by_member_query, date_cols=['vote_date'])
    save_data(votes_by_member_df, "streamlit_data/votes_by_member.parquet")