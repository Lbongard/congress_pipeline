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


def query_options_by_zip(zip_code):
    query = f"""
                select distinct mems.name, 
                                mems.state, 
                                dists.congressional_district,
                                mems.bioguideID
                from `Congress.dim_members` mems join `Congress.dim_congressional_districts` dists
                on (mems.district = dists.congressional_district) and (mems.state = dists.state)
                where mems.most_recent_chamber = 'House of Representatives' AND dists.zip_code = {zip_code}
                order by mems.state ASC
    """
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results


def query_additional_data(selected_option):
    query = f"""
    SELECT *
    FROM `Congress.dim_members`
    WHERE bioguideID = '{selected_option}'
    """
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results


st.title('BigQuery Data Selector')

# Step 1: Enter zip code
zip_code = st.text_input('Enter your zip code:')

if zip_code:
    # Step 2: Query options based on zip code
    options_df = query_options_by_zip(zip_code)
    options = options_df['option_column_name'].tolist()
    
    if options:
        selected_option = st.selectbox('Select an option:', options)
        
        if selected_option:
            # Step 3: Query additional data based on selected option
            additional_data_df = query_additional_data(selected_option)
            st.write('Additional Data:')
            st.write(additional_data_df)
    else:
        st.write('No options found for this zip code.')