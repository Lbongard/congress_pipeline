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
import numpy as np

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

# Define columns
col = st.columns((6, 2))


##### Functions for Querying Selection Options #####


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


@st.cache_data
def query_voting_records():
    voting_record_query = f"""
        select *
        from Congress.votes_by_member
    """
    
    query_job = client.query(voting_record_query)
    results = query_job.result().to_dataframe()
    
    return results

voting_results = query_voting_records()

def vote_cnt_by_member(x):
    return (x != 'not_voting').sum()

def calculate_votes_w_party(results, chamber, policy_area=None):
    if policy_area:
        df = results[(results.policy_area == policy_area) & (results.chamber == chamber)]
    else:
        df = results[results.chamber == chamber]
    
    grouped_df = grouped_df = df.groupby(['bioguideID', 'name', 'partyName']).agg({
    'voted_with_party_majority':['sum'],
    'member_vote':[vote_cnt_by_member]
    })
    
    percentage_df = grouped_df['voted_with_party_majority']['sum'] / grouped_df['member_vote']['vote_cnt_by_member']
    percentage_df.rename("% of Votes with Party Majority", inplace=True)
    percentage_df = pd.DataFrame(percentage_df).reset_index()

    # percentage_df['partyName'] = pd.Categorical(percentage_df['partyName'], 
    #                                             categories=['Democratic', 'Republican', 'Independent'], 
    #                                             ordered=True)
    
    # percentage_df = percentage_df.sort_values('partyName')
    
    return percentage_df


def plot_voting_records(results, bioguideID, chamber, policy_area=None):
    votes_with_party_all = calculate_votes_w_party(results=results, chamber=chamber, policy_area=policy_area)

    # Format percentages
    votes_with_party_all['% of Votes with Party Majority'] = votes_with_party_all['% of Votes with Party Majority'].apply(lambda x: f"{x:.2%}")
    
    if policy_area:
        vote_count = results[(results.policy_area == policy_area) & (results.bioguideID == bioguideID)].shape[0]
    else:
        vote_count = results[(results.bioguideID == bioguideID)].shape[0]

    if chamber == 'House of Representatives':
        rep_title = 'Rep.'
    else:
        rep_title = 'Senator'
    
    highlight_point = votes_with_party_all[votes_with_party_all['bioguideID'] == bioguideID]
    rep_name = highlight_point.iloc[0]['name']
    party_maj_votes = float(highlight_point.iloc[0]['% of Votes with Party Majority'][:-1])/100

    color_mapping = {
    'Democratic': 'blue',
    'Republican': 'red',
    'Independent': 'orange',
}

    # Create a strip (scatter) plot
    fig = px.strip(votes_with_party_all[votes_with_party_all['bioguideID'] != bioguideID], # Member will be plotted via highlight_point below
                   y='partyName', 
                   x='% of Votes with Party Majority', 
                   color='partyName',
                   color_discrete_map=color_mapping, 
                   labels={'name':'Name', 'partyName': 'Party'},
                   hover_data={'name': True, 'partyName': True, '% of Votes with Party Majority':True},
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
            marker=dict(color='#9467bd', size=15, symbol='star')  # Highlighted point style
        ))

    fig.update_layout(
        title=dict(
            text=f"{rep_title} {rep_name.split(',')[0]} votes with their party in {party_maj_votes:.0%} of votes"
                 f"{' related to ' + policy_area if policy_area else ''}"
                 f" (n = {vote_count} votes)",
            x=0.5,  
            xanchor='center',
            yanchor='top',  
            font=dict(size=14)  
    ),
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

@st.cache_data
def query_sponsored_bills():
    sponsored_bills_query = f"""
        select *
        from Congress.vw_sponsored_bills_by_member
    """
    
    query_job = client.query(sponsored_bills_query)
    results = query_job.result().to_dataframe()
    
    return results

sponsored_bills = query_sponsored_bills()

@st.cache_data
def query_term_data():

    query = f"""
    SELECT chamber,
           term_start_year, 
           term_end_year,
           bioguideID
    FROM `Congress.dim_terms`
    order by term_start_year
    """
    
    query_job = client.query(query)
    results = query_job.result().to_dataframe()

    results['term_end_year'] = results['term_end_year'].astype(object).fillna('-')
    results.set_index('chamber', inplace=True)
    results.rename({'term_start_year': 'Start Year',
                    'term_end_year': 'End Year'},
                    axis=1,
                    inplace=True)
    return results

terms_df = query_term_data()

@st.cache_data
def query_policy_areas():
    query = f"""
    SELECT distinct policy_area
    FROM `Congress.dim_bills`
    WHERE bill_key in (SELECT bill_key FROM `Congress.fact_roll_call_vote`)
    """
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results


policy_areas = query_policy_areas()



###### Start Streamlit Page ######
with st.sidebar:

    st.title('Congress Member Dashboard')

    selected_method = st.selectbox('Select a method:', ['Look up my Representative', 'Search Representative by Name'],
                                index=None)
    # Initialize geo and last_name to None
    geo                   = None
    last_name             = None
    options_df            = None
    selected_option       = None
    additional_data_df    = None
    policy_area           = None
    policy_area_selection = None

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


    
    if selected_method == 'Look up my Representative':

        chambers = ['Senate', 'House of Representatives']
        selected_chamber = st.selectbox('Select a chamber of Congress:', 
                                        chambers, 
                                        index=None)

        # Step 1: Enter zip code or representative
        if selected_chamber == 'House of Representatives':
            geo = st.text_input('Enter your zip code:')
        elif selected_chamber == 'Senate':
            geo = st.selectbox('Select your state:', 
                            states,
                            index=None)
        else:
            pass
    
        if geo:
            try:
                options_df = query_options_by_geo(chamber=selected_chamber, geo=geo)
            except Exception as e:
                st.write('An error occurred. Please ensure that your entry is valid.')
                st.stop()
    
    
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

        # try:
        #     options_df = query_options_by_geo(chamber=selected_chamber, geo=geo)
        # except Exception as e:
        #     st.write('An error occurred. Please ensure that your entry is valid.')
        #     st.stop()
    
    
    if options_df is not None:
        options = options_df['concat_name'].tolist()
        selected_option = st.selectbox('Select a representative:', 
                                    options,
                                    index=None)
        
    

with col[0]:
        
    if selected_option:
        with st.spinner('Loading Data'):
            rep_name = selected_option.split(' | ')[0]
                
            try:
                additional_data_df = query_by_name(rep_name)
            except Exception as e:
                st.write('An error occurred.')

            chamber = additional_data_df.iloc[0]['most_recent_chamber']
            bioguideID = additional_data_df.iloc[0]['bioguideID']

            policy_areas = voting_results\
                                    [(voting_results.bioguideID == bioguideID) & (voting_results.policy_area.isna() == False)]\
                                    ['policy_area'].\
                                     value_counts().sort_index()
            
            formatted_policy_list = [f"{item} | (n={count})" for item, count in policy_areas.items()]

            policy_area_selection = st.selectbox("Optional - Filter results by bill subject", 
                                           formatted_policy_list,
                                           index=None,
                                           key='policy_area_selection')
            
            def reset_selection():
                st.session_state.policy_area_selection = None

            st.button("Clear Policy Area Selection", on_click=reset_selection)
            
            if policy_area_selection:
                policy_area = policy_area_selection.split(' | ')[0]
            
            # voting_records = query_voting_records(chamber=chamber)
            fig = plot_voting_records(results=voting_results, 
                                      bioguideID=bioguideID, 
                                      chamber=chamber,
                                      policy_area=policy_area
                                      )
            st.plotly_chart(fig, use_container_width=False)

            if policy_area_selection:
                sponsored_bills_table_data = sponsored_bills[(sponsored_bills['bioguideID'] == bioguideID) & (sponsored_bills['policy_area'] == policy_area)]
            else:
                sponsored_bills_table_data = sponsored_bills[sponsored_bills['bioguideID'] == bioguideID]
            
            # sponsored_bills_table_data = sponsored_bills_table_data[['title']]

            if sponsored_bills_table_data.empty:
                st.text('No sponsored legislation found for this member.')
            else:
                st.text('Recent Sponsored Legislation')
                st.table(sponsored_bills_table_data.sort_values('introducedDate').head(25))

            # terms_df = query_term_data(bioguideID)
            # fig_terms = plot_terms(dict(terms_df))

            # st.plotly_chart(fig_terms, use_container_width=False)



with col[1]:
    if additional_data_df is not None:

        # Add Image
        imageURL = additional_data_df.iloc[0]['imageURL']
        st.image(imageURL,
                caption=selected_option)        
        
        st.text('Years Served')
        terms_table_data = terms_df[terms_df.bioguideID == bioguideID][['Start Year', 'End Year']]
        st.table(terms_table_data)
        

    
