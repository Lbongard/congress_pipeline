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
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from agstyler.agstyler import PINLEFT, PRECISION_TWO, draw_grid, highlight_mult_colors, cellRenderer
from pyarrow import parquet
import gcsfs

config_path = os.path.abspath(os.getenv('TF_VAR_google_credentials'))

bucket_name = os.getenv("streamlit_data_bucket_name")

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

# Set os.environ for download from gcs bucket
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('TF_VAR_google_credentials')


tab1, tab2, tab3 = st.tabs(["Overall Voting Record", "Recent Votes", "Sponsored Bills"])

# Define columns
# col = st.columns((6, 2))


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
def get_update_date():
    update_date_query = "SELECT CURRENT_DATE('America/Los_Angeles') as update_date"

    query_job = client.query(update_date_query)
    results = query_job.result().to_dataframe()
     
    return results


@st.cache_data(ttl=86400) # Cache for 24 hours
def read_parquet_from_gcs(gcs_uri):
    # Load the parquet files into a pandas DataFrame
    df = pd.read_parquet(gcs_uri, engine='pyarrow', storage_options={"token": None})
    return df


voting_results_uri = f"gs://{bucket_name}/votes_by_member.parquet"
voting_results = read_parquet_from_gcs(voting_results_uri)
voting_results['title_linked'] = voting_results.apply(
                lambda row: f'<a href="{row["url"]}" target="_blank">{row["title"]}</a>', axis=1
                )

sponsored_bills_uri = f"gs://{bucket_name}/sponsored_bills_by_member.parquet"
sponsored_bills = read_parquet_from_gcs(sponsored_bills_uri)


# @st.cache_data(ttl=86400) # Cache for 24 hours
# def query_voting_records(path):
#     df = pd.read_parquet(path)
    
#     df['title_linked'] = df.apply(
#                 lambda row: f'<a href="{row["url"]}" target="_blank">{row["title"]}</a>', axis=1
#                 )
#     return pd.DataFrame(df)

# voting_results = query_voting_records('streamlit_data/votes_by_member.parquet')

def vote_cnt_by_member(x):
    return (x != 'not_voting').sum()

def calculate_votes_w_party(results, chamber, policy_area=None):
    if policy_area:
        df = results[(results.policy_area == policy_area) & (results.chamber == chamber)]
    else:
        df = results[results.chamber == chamber]
    
    grouped_df = df.groupby(['bioguideID', 'name', 'partyName']).agg({
    'voted_with_party_majority':['sum'],
    'voted_with_dem_majority':['sum'],
    'voted_with_rep_majority':['sum'],
    'member_vote':[vote_cnt_by_member]
    })
    
    percentage_df = pd.DataFrame({"% of Votes with Party Majority" : grouped_df['voted_with_party_majority']['sum'] / grouped_df['member_vote']['vote_cnt_by_member'],
                                  "% of Votes with Dem Majority" : grouped_df['voted_with_dem_majority']['sum'] / grouped_df['member_vote']['vote_cnt_by_member'],
                                  "% of Votes with Rep Majority" : grouped_df['voted_with_rep_majority']['sum'] / grouped_df['member_vote']['vote_cnt_by_member']
                                  })
    percentage_df.reset_index(inplace=True)

    # percentage_df.rename("% of Votes with Party Majority", inplace=True)
    # percentage_df = pd.DataFrame(percentage_df).reset_index()

    return percentage_df


def plot_voting_records(results, bioguideID, chamber, policy_area=None):
    id_cols = ['bioguideID', 'name', 'partyName']

    # Set the columns to be used for plotting
    df_cols = ['% of Votes with Dem Majority', '% of Votes with Rep Majority']
    
    votes_with_party_all = calculate_votes_w_party(results=results, chamber=chamber, policy_area=policy_area)
    
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
    dem_majority_votes = highlight_point.iloc[0]['% of Votes with Dem Majority']
    rep_majority_votes = highlight_point.iloc[0]['% of Votes with Rep Majority']

    highlight_points = {
        'x': rep_majority_votes,
        'y': dem_majority_votes,
        'party': highlight_point.iloc[0]['partyName'],
        'name': rep_name
    }

    color_mapping = {
        'Democratic': 'blue',
        'Republican': 'red',
        'Independent': 'orange',
    }

    plot_df = votes_with_party_all[(votes_with_party_all['bioguideID'] != bioguideID)]
    
    # Create a scatter plot
    fig = px.scatter(plot_df, 
                     x='% of Votes with Rep Majority', 
                     y='% of Votes with Dem Majority', 
                     color='partyName',
                     color_discrete_map=color_mapping, 
                     labels={'name':'Name', 'partyName': 'Party'},
                     hover_data={
                         'name': True, 
                         'partyName': True, 
                         '% of Votes with Rep Majority': ':.0%', 
                         '% of Votes with Dem Majority': ':.0%'
                     })

    fig.add_trace(go.Scatter(
        x=[highlight_points['x']], 
        y=[highlight_points['y']],  
        mode='markers',
        name=f"{highlight_points['name']}",
        customdata=[[highlight_points['name']]],  # Add name to customdata
        text=[f"{highlight_points['name']}"],  # Add name to text
        hovertemplate='<b>%{customdata[0]}</b><br>' +
                      '%{y:.0%} of Votes with Dem Majority<br>' +
                      '%{x:.0%} of Votes with Rep Majority<br>' +
                      '<extra></extra>',
        marker=dict(color='#9467bd', size=15, symbol='star')  
    ))

    fig.update_layout(
        title=dict(
            text=f"{rep_title} {rep_name.split(',')[0]} votes with Democrats {dem_majority_votes:.0%} of the time and Republicans {rep_majority_votes:.0%} of the time"
                 f"{' on bills related to ' + policy_area if policy_area else ''}"
                 f" (n = {vote_count} votes)",
            x=0.5,  
            xanchor='center',
            yanchor='top',  
            font=dict(size=14)  
        ),
        yaxis_title='% of Votes with Dem Majority',
        xaxis_title='% of Votes with Rep Majority',
        legend_title='Party',
        height=450,
        width=850,
        margin=dict(l=50, r=50, t=50, b=50),  # Adjust margins for better spacing
        xaxis_tickformat='.0%',  # Format x-axis as percentage
        yaxis_tickformat='.0%'   # Format y-axis as percentage
    )

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

# @st.cache_data(ttl=86400) # Cache for 24 hours
# def query_sponsored_bills(path):
#     df = pd.read_parquet(path)
#     return df

# sponsored_bills = query_sponsored_bills('streamlit_data/sponsored_bills_by_member.parquet')

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

def display_term_summary(additional_data_df, terms_df, mem_name, bioguideID):
    imageURL = additional_data_df.iloc[0]['imageURL']
    st.image(imageURL,
            caption=mem_name)        
    
    st.text('Years Served')
    terms_table_data = terms_df[terms_df.bioguideID == bioguideID][['Start Year', 'End Year']]
    st.table(terms_table_data)




###### Start Streamlit Page ######
with st.sidebar:

    st.title('Congress Member Info App')

    chambers = ['Senate', 'House of Representatives']
    selected_chamber = st.selectbox('Select a chamber of Congress:', 
                                    chambers, 
                                    index=None)

    selected_method = st.selectbox('Select a search method:', ['Search Member by State, Zip Code', 'Search Member by Name'],
                                index=None)
    # Initialize geo and last_name to None
    geo                   = None
    last_name             = None
    options_df            = None
    selected_option       = None
    additional_data_df    = None
    policy_area           = None
    policy_area_selection_voting_record = None
    policy_area_selection_indiv_votes = None
    indiv_vote_display_options = None
    bill_keyword_indiv_votes = None

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


    
    if selected_method == 'Search Member by State, Zip Code':

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
    
    
    if selected_method == 'Search Member by Name':

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
        
    if selected_option:
            rep_name = selected_option.split(' | ')[0] 
            try:
                additional_data_df = query_by_name(rep_name)
            except Exception as e:
                st.write('An error occurred.')
            
            chamber = additional_data_df.iloc[0]['most_recent_chamber']
            bioguideID = additional_data_df.iloc[0]['bioguideID']
            partyName = additional_data_df.iloc[0]['partyName']
            policy_areas = voting_results\
                                    [(voting_results.bioguideID == bioguideID) & (voting_results.policy_area.isna() == False)]\
                                    ['policy_area'].\
                                    value_counts().sort_index()

with tab1:
    col = st.columns((6, 2))
    with col[0]:       
        if selected_option:
            
            formatted_policy_list = [f"{item} | (n={count})" for item, count in policy_areas.items()]

            policy_area_selection_voting_record = st.selectbox("Optional - Filter results by bill subject", 
                                        formatted_policy_list,
                                        index=None,
                                        key='policy_area_selection_voting_record')
            
            def reset_selection():
                st.session_state.policy_area_selection_voting_record = None

            # st.button("Clear Policy Area Selection", on_click=reset_selection)
            
            if policy_area_selection_voting_record:
                policy_area = policy_area_selection_voting_record.split(' | ')[0]
            
            fig = plot_voting_records(results=voting_results, 
                                    bioguideID=bioguideID, 
                                    chamber=chamber,
                                    policy_area=policy_area
                                    )
            st.plotly_chart(fig, use_container_width=False)
            



    with col[1]:
        if additional_data_df is not None:

            display_term_summary(additional_data_df=additional_data_df, 
                                 terms_df=terms_df, 
                                 mem_name=selected_option, 
                                 bioguideID=bioguideID)

            # # Add Image
            # imageURL = additional_data_df.iloc[0]['imageURL']
            # st.image(imageURL,
            #         caption=selected_option)        
            
            # st.text('Years Served')
            # terms_table_data = terms_df[terms_df.bioguideID == bioguideID][['Start Year', 'End Year']]
            # st.table(terms_table_data)

with tab2:
    col = st.columns((6, 2))
    with col[0]:
        if selected_option:
            
            indiv_vote_display_options = st.radio("Options:", ['See Recent Votes', 'Search Votes by Bill Keyword'])

            if indiv_vote_display_options == 'Search Votes by Bill Keyword':
                bill_keyword_indiv_votes = st.text_input("Enter a Bill Keyword:")
                keyword_filter = voting_results['title'].str.lower().str.contains(bill_keyword_indiv_votes.lower())
            else:
                bill_keyword_indiv_votes = None
                keyword_filter = True

            policy_area_selection_indiv_votes = st.selectbox("Optional - Filter results by bill subject", 
                            formatted_policy_list,
                            index=None,
                            key='policy_area_selection_indiv_votes')
            
            if policy_area_selection_indiv_votes:
                policy_area = policy_area_selection_indiv_votes.split(' | ')[0]
                policy_area_filter = voting_results['policy_area'] == policy_area
            else:
                policy_area_filter = True
            
            formatted_policy_list = [f"{item} | (n={count})" for item, count in policy_areas.items()]


                
            # def reset_selection():
            #     st.session_state.policy_area_selection_recent_votes = None

            # st.button("Clear Policy Area Selection", on_click=reset_selection)
                
            # Display table of recent votes
            display_cols = [
                'vote_date', 'title', 'url', 'roll_call_number', 'policy_area', 'member_vote', 'dem_majority_vote', 'rep_majority_vote', 'result', 'partyName']
            
            # Account for IF THEN LOGIC

            
            dem_vote_match_condition = "params.data.member_vote === params.data.dem_majority_vote"
            rep_vote_match_condition = "params.data.member_vote === params.data.rep_majority_vote"
            
            if partyName == 'Democratic':
                
                dem_cell_style = \
                highlight_mult_colors(primary_color="#abf7b1", # Light Green
                                    secondary_color="#fcccbb", # Light Red
                                    condition=dem_vote_match_condition
                                        )
                mem_cell_style = dem_cell_style # Replicating formatting for member vote
                rep_cell_style = None # Do not format Rep vote cell

            elif partyName == 'Republican':

                rep_cell_style = \
                highlight_mult_colors(primary_color="#abf7b1", # Light Green
                                    secondary_color="#fcccbb", # Light Red
                                    condition=rep_vote_match_condition
                                        )
                mem_cell_style = rep_cell_style # Replicating formatting for member vote
                dem_cell_style = None

            else:
                mem_cell_style = None
                dem_cell_style = \
                highlight_mult_colors(primary_color="#abf7b1", # Light Green
                                    secondary_color="#fcccbb", # Light Red
                                    condition=dem_vote_match_condition
                                        )
                rep_cell_style = \
                highlight_mult_colors(primary_color="#abf7b1", # Light Green
                                    secondary_color="#fcccbb", # Light Red
                                    condition=rep_vote_match_condition
                                        )                  
            
            formatter = {
            'title': ('Title (Click for more info)', {'width': 250, 'wrapText': True, 'autoHeight': True, 'cellRenderer':cellRenderer}),
            # 'url': ('Link', {'cellRenderer':cellRenderer}),
            'member_vote': ('Member Vote', {'width': 115, 'autoHeight': True, 'cellStyle':mem_cell_style}),
            'dem_majority_vote': ('Dem Maj Vote', {'width': 115, 'autoHeight': True, 'cellStyle':dem_cell_style}),
            'rep_majority_vote': ('Rep Maj Vote', {'width': 115, 'autoHeight': True, 'cellStyle':rep_cell_style}),
            'result': ('Result', {'width': 125}),
            'policy_area': ('Policy Area', {'width': 150}),
            'roll_call_number': ('Roll Call Vote', {'width': 110})
            }
            
            voting_results_table_data = voting_results[(voting_results['bioguideID'] == bioguideID) & \
                                                       (keyword_filter) & \
                                                       (policy_area_filter) & \
                                                       (voting_results['url'].isna() == False)]\
                                        [display_cols].\
                                        set_index('vote_date').\
                                        sort_index(ascending = False)\
                                        # [display_cols]

            st.title('Recent Voting Results')
            if voting_results_table_data.empty:
                st.text("No Votes to Display")
            else:
                data = draw_grid(
                    voting_results_table_data,
                    formatter=formatter,
                    # fit_columns=True,
                    selection='single', 
                    max_height=300,
                    grid_options={'domLayout':'normal',
                                'enableCellTextSelection':True}
                )



    with col[1]:
        if additional_data_df is not None:
            display_term_summary(additional_data_df=additional_data_df, 
                                 terms_df=terms_df, 
                                 mem_name=selected_option, 
                                 bioguideID=bioguideID)
        
with tab2:
    col = st.columns((6, 2))
    with col[0]:
        if selected_option:
            if policy_area_selection_voting_record:
                sponsored_bills_table_data = sponsored_bills[(sponsored_bills['bioguideID'] == bioguideID) & (sponsored_bills['policy_area'] == policy_area)]
            else:
                policy_area = None
                sponsored_bills_table_data = sponsored_bills[sponsored_bills['bioguideID'] == bioguideID]
            
            # sponsored_bills_table_data = sponsored_bills_table_data[['title']]

            if sponsored_bills_table_data.empty:
                st.text('No sponsored legislation found for this member.')
            else:
                st.text('Recent Sponsored Legislation')
                st.table(sponsored_bills_table_data.sort_values('introducedDate', ascending = False).head(25))

            # terms_df = query_term_data(bioguideID)
            # fig_terms = plot_terms(dict(terms_df))

            # st.plotly_chart(fig_terms, use_container_width=False)
