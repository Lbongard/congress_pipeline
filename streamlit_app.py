# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import os
import json
import altair as alt
import plotly.express as px
from streamlit_searchbox import st_searchbox
import plotly.graph_objs as go
import pandas as pd

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


def query_options_by_geo(chamber, geo):
    
    if chamber == 'Senate':
        geo_filter = f"mems.state = '{geo}'"
        dist_join_condition = ''
        votes_filter = 'mems.lisid in (SELECT distinct lisid from `Congress.fact_roll_call_vote`)'
    elif chamber == 'House of Representatives':
        geo_filter = f'dists.zip_code = {geo}'
        dist_join_condition = '(COALESCE(mems.district, 0) = dists.congressional_district) and'
        votes_filter = 'mems.bioguideID in (SELECT distinct bioguideID from `Congress.fact_roll_call_vote`)'

    query = f"""
                with distinct_members_cte as(
                select distinct  mems.name 
                                ,mems.state 
                                ,mems.bioguideID
                                ,mems.imageURL
                                ,mems.partyName
                from `Congress.dim_members` mems join `Congress.dim_congressional_districts` dists
                on {dist_join_condition} (mems.state = dists.state)
                where mems.most_recent_chamber = '{chamber}' 
                      AND {geo_filter}
                      AND {votes_filter}
                order by mems.state ASC)

                select concat(name, " | ", partyName, " - ", state) concat_name
                from distinct_members_cte
    """
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results

def query_options_by_name(chamber, search_name):
    
    if chamber == 'Senate':
        votes_filter = 'mems.lisid in (SELECT distinct lisid from `Congress.fact_roll_call_vote`)'
    elif chamber == 'House of Representatives':
        votes_filter = 'mems.bioguideID in (SELECT distinct bioguideID from `Congress.fact_roll_call_vote`)'

    query = f"""
                with distinct_members_cte as(
                select distinct  name 
                                ,state 
                                ,bioguideID
                                ,imageURL
                                ,partyName
                from `Congress.dim_members` mems 
                where mems.most_recent_chamber = '{chamber}' 
                      AND name LIKE '%{search_name}%'
                      AND {votes_filter}
                order by mems.state ASC)

                select concat(name, " | ", partyName, " - ", state) concat_name
                from distinct_members_cte
    """
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results



def query_voting_records(chamber):
    voting_record_query = f"""
        select *
        from Congress.votes_w_majority_by_member
        where chamber = '{chamber}'
    """
    
    query_params = [
        bigquery.ScalarQueryParameter("chamber", "STRING", chamber)
    ]
    
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    query_job = client.query(voting_record_query, job_config=job_config)
    results = query_job.result().to_dataframe()
    
    return results


def plot_voting_records(results, bioguideID):
    df_all_mems = results[['bioguideID', 'name', 'partyName', 'perc_votes_with_party_majority']]
    votes_with_party_all = pd.DataFrame(df_all_mems.groupby(['bioguideID', 'name', 'partyName'])['perc_votes_with_party_majority'].mean()).reset_index()
    votes_with_party_all.rename(columns={'perc_votes_with_party_majority': '% of Votes with Party Majority'}, inplace=True)

    if chamber == 'House of Representatives':
        rep_title = 'Rep.'
    else:
        rep_title = 'Senator'
    
    highlight_point = votes_with_party_all[votes_with_party_all['bioguideID'] == bioguideID]
    rep_name = highlight_point.iloc[0]['name']
    party_maj_votes = highlight_point.iloc[0]['% of Votes with Party Majority']

    # Create a strip (scatter) plot
    fig = px.strip(votes_with_party_all, 
                   y='partyName', 
                   x='% of Votes with Party Majority', 
                   color='partyName',
                   labels={'name':'Name', 'partyName': 'Party'},
                   hover_data={'name': True, 'partyName': True, '% of Votes with Party Majority': True},
                   orientation='h',  # Set orientation to horizontal (default is vertical)
                   stripmode='overlay')  # Overlay points when they overlap

    
    if not highlight_point.empty:
        fig.add_trace(go.Scatter(
            y=[highlight_point.iloc[0]['partyName']],  # Ensure y is a list or array-like
            x=[highlight_point.iloc[0]['% of Votes with Party Majority']],  # Ensure x is a list or array-like
            mode='markers',
            name=f"{highlight_point.iloc[0]['name']}",
            text=f"Name: {rep_name}<br>% of Votes with Party Majority: {party_maj_votes: .0%}",
            hoverinfo='text',
            marker=dict(color='purple', size=15, symbol='star')  # Highlighted point style
        ))

    fig.update_layout(
        title=f"{rep_title} {rep_name.split(',')[0]} votes with their party in {party_maj_votes:.0%} of votes",
        yaxis_title='Party',
        xaxis_title='% of Votes with Party Majority',
        legend_title='Party'
    )

    # fig.show()
    return fig


def query_by_name(selected_option):
    query = f"""
    SELECT *
    FROM `Congress.dim_members`
    WHERE name = '{selected_option}'
    """
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results


def query_members(selected_option):
    query = f"""
    SELECT distinct name
    FROM `Congress.dim_members`
    WHERE name LIKE '%{selected_option}%'
    """
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results['name'].tolist()


###### Start Streamlit Page ######
st.title('Congress Member Dashboard')

selected_method = st.selectbox('Select a method:', ['Look up my Representative', 'Search Representative by Name'],
                               index=None)
# Initialize geo to None
geo = None

st.markdown("---")

states = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]


##### Lookup by Geography Option #####
if selected_method == 'Look up my Representative':

    chambers = ['Senate', 'House of Representatives']
    selected_chamber = st.selectbox('Select a chamber of Congress:', 
                                    chambers, 
                                    index=None)

    # Step 1: Enter zip code or representative
    if selected_chamber == 'House of Representatives':
        geo = st.text_input('Enter your zip code:')
    else:
        geo = st.selectbox('Select your state:', 
                           states,
                           index=None)

if geo and selected_method == 'Look up my Representative':
    
    try:
        options_df = query_options_by_geo(chamber=selected_chamber, geo=geo)
    except Exception as e:
        st.write('An error occurred. Please ensure that your entry is valid.')
        st.stop()

    if options_df.empty:
        st.write('No representative found for this entry. Please ensure that your entry is valid.')

    options = options_df['concat_name'].tolist()
    
    if options:
        selected_option = st.selectbox('Select a representative:', 
                                       options,
                                       index=None)
        
        if selected_option:
            with st.spinner('Loading Data'):
                rep_name = selected_option.split(' | ')[0]
                additional_data_df = query_by_name(rep_name)

                st.write('Additional Data:')
                imageURL = additional_data_df.iloc[0]['imageURL']
                st.image(imageURL)
                
                chamber = additional_data_df.iloc[0]['most_recent_chamber']
                bioguideID = additional_data_df.iloc[0]['bioguideID']
                voting_records = query_voting_records(chamber=chamber)
                fig = plot_voting_records(voting_records, bioguideID)
                st.plotly_chart(fig, use_container_width=True)


#### Lookup by Name Option #####
if selected_method == 'Search Representative by Name':
    chambers = ['Senate', 'House of Representatives']
    selected_chamber = st.selectbox('Select a chamber of Congress:', 
                                    chambers, 
                                    index=None)

    first_name = st.text_input("Enter your representative's first name (Optional)")
    last_name = st.text_input("Enter your representative's last name:")

    if last_name:
        search_name = last_name + ", " + first_name
        
        try:
            options_df = query_options_by_name(chamber=selected_chamber, search_name=search_name)
        except Exception as e:
            st.write('An error occurred. Please ensure that your entry is valid.')
            st.stop()
        
        if options_df.empty:
            st.write('No representative found for this entry. Please ensure that your entry is valid.')

        options = options_df['concat_name'].tolist()
    
        if options:
            selected_option = st.selectbox('Select a representative:', 
                                        options,
                                        index=None)
            
            if selected_option:
                with st.spinner('Loading Data'):
                    rep_name = selected_option.split(' | ')[0]
                    additional_data_df = query_by_name(rep_name)

                    st.write('Additional Data:')
                    imageURL = additional_data_df.iloc[0]['imageURL']
                    st.image(imageURL)
                    
                    chamber = additional_data_df.iloc[0]['most_recent_chamber']
                    bioguideID = additional_data_df.iloc[0]['bioguideID']
                    voting_records = query_voting_records(chamber=chamber)
                    fig = plot_voting_records(voting_records, bioguideID)
                    st.plotly_chart(fig, use_container_width=True)


        

    
