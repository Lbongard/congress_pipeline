# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import os
import json
import altair as alt
import plotly.express as px

config_path = os.path.abspath(os.getenv('TF_VAR_google_credentials'))

with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Set Page Config
st.set_page_config(
    page_title="US Congress Dashboard",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# Perform query.
sql = "SELECT * FROM `Congress.fact_roll_call_vote` LIMIT 10"
project_id='testproject-420401'
# # Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl=600)
# def run_query(query):
#     query_job = client.query(query)
#     rows_raw = query_job.result()
#     # Convert to list of dicts. Required for st.cache_data to hash the return value.
#     rows = [dict(row) for row in rows_raw]
#     return rows

# rows = run_query("SELECT * FROM `Congress.fact_roll_call_vote` LIMIT 10")


df = pandas_gbq.read_gbq(sql, project_id=project_id)


# Print results.
st.write("Recent votes:")
for row in df.iterrows():
    st.write(row)