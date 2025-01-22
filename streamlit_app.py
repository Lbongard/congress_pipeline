# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery, storage
from google.cloud import secretmanager
import pandas_gbq
import os
import json
import altair as alt
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from agstyler.agstyler import PINLEFT, PRECISION_TWO, draw_grid, highlight_mult_colors, cellRenderer
from pyarrow import parquet
from datetime import datetime
import pytz
import gcsfs
import base64


PROJECT_ID = os.path.abspath(os.getenv('TF_VAR_gcp_project'))

# Function to access secret from Secret Manager
def get_secret(project_num, secret_name):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_num}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    secret = response.payload.data.decode("UTF-8")
    return json.loads(secret)



# Check if the app is running locally or on Cloud Run
if os.getenv("LOCAL_RUN"):  # Local environment
    config_path = os.path.abspath(os.getenv('TF_VAR_google_credentials'))
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('TF_VAR_google_credentials')

# else:  # Cloud Run environment
    # Retrieve service account credentials from Secret Manager in Cloud Run
    # project_num = os.getenv('project_num')
    # creds_secret_name = os.getenv('gcp_service_account_credentials')
    # service_account_info = get_secret(project_num=project_num, secret_name=creds_secret_name)
    # credentials = service_account.Credentials.from_service_account_info(service_account_info)

    # encoded_credentials = st.secrets["gcp"]["credentials_json"]
    # credentials_json = base64.b64decode(encoded_credentials)

    # with open("gcp_credentials.json", "wb") as f:
    #     f.write(credentials_json)

    # gcp_credentials = json.loads(os.getenv("GCP_CREDENTIALS_JSON", "{}"))

    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credentials

# Create API client using the service account

# client = bigquery.Client(credentials=credentials)
client = bigquery.Client()

bucket_name = os.getenv("streamlit_data_bucket_name")

# Set Page Config
st.set_page_config(
    page_title="US Congress Dashboard",
    layout="wide",
    initial_sidebar_state="expanded")

# Set os.environ for download from gcs bucket



tab1, tab2, tab3 = st.tabs(["Overall Voting Record", "Recent Votes", "Sponsored Bills"])


# Define the PST timezone for displaying last updated datetime
pst = pytz.timezone("America/Los_Angeles")

@st.cache_data(ttl=86400) # Cache for 24 hours
def read_parquet_from_gcs(gcs_uri):
    # Load the parquet files into a pandas DataFrame
    df = pd.read_parquet(gcs_uri, engine='pyarrow', storage_options={"token": None})
    last_updated_utc = datetime.now(pytz.utc)
    last_updated_pst = last_updated_utc.astimezone(pst).strftime("%Y-%m-%d %H:%M:%S")
    return df, last_updated_pst


voting_results, _ = read_parquet_from_gcs(f"gs://{bucket_name}/vw_votes_by_member.parquet")
member_options, _ = read_parquet_from_gcs(f"gs://{bucket_name}/vw_voting_members.parquet")
dim_members, _ = read_parquet_from_gcs(f"gs://{bucket_name}/dim_members.parquet")
sponsored_bills, _ = read_parquet_from_gcs(f"gs://{bucket_name}/vw_sponsored_bills_by_member.parquet")
terms_df, _ = read_parquet_from_gcs(f"gs://{bucket_name}/vw_terms_condensed.parquet")
dim_congressional_districts, last_updated = read_parquet_from_gcs(f"gs://{bucket_name}/dim_congressional_districts.parquet")

## Post Processing on imported dataframes

# Applying formatting to link title to bill in table
voting_results['title_linked'] = voting_results.apply(
                lambda row: f'<a href="{row["url"]}" target="_blank">{row["title"]}</a>', axis=1
                )
sponsored_bills['introducedDate'] = pd.to_datetime(sponsored_bills['introducedDate']).dt.strftime('%Y-%m-%d')

terms_df['term_end_year'] = terms_df['term_end_year'].apply(lambda x: int(round(x)) if pd.notnull(x) else '-')
# terms_df['term_end_year'] = terms_df['term_end_year'].astype(object).fillna('-')
terms_df.set_index('chamber', inplace=True)
terms_df.rename({'term_start_year': 'Start Year',
                'term_end_year': 'End Year'},
                axis=1,
                inplace=True)



def vote_cnt_by_member(x):
    return (x != 'Not Voting').sum()

def calculate_votes_w_party(results, chamber, policy_area=None):
    if policy_area:
        df = results[(results.policyArea == policy_area) & (results.chamber == chamber)]
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
        vote_count = results[(results.policyArea == policy_area) & (results.bioguideID == bioguideID)].shape[0]
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
            text=f"{rep_title} {rep_name.split(',')[0]}'s vote align with the Democratic majority {dem_majority_votes:.0%} of the time <br>and the Republican majority {rep_majority_votes:.0%} of the time"
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

def display_term_summary(additional_data_df, terms_df, mem_name, bioguideID):
    imageURL = additional_data_df.iloc[0]['imageURL']
    if imageURL:
            st.image(imageURL,
                     caption=mem_name)        
    
    st.text('Years Served')
    terms_table_data = terms_df[terms_df.bioguideID == bioguideID][['Start Year', 'End Year']]
    st.table(terms_table_data)


###### Start Streamlit Page ######
with st.sidebar:
    
    # Initialize selection options to None
    congress_session      = None
    selected_method       = None
    geo                   = None
    last_name             = None
    options_df            = None
    selected_option       = None
    additional_data_df    = None
    policy_area_voting_record = None
    policy_area_indiv_votes = None
    policy_area_sponsored_bills = None
    policy_area_selection_voting_record = None
    policy_area_selection_indiv_votes = None
    policy_area_selection_sponsored_bills = None
    indiv_vote_display_options = None
    bill_keyword_indiv_votes = None
    sponsored_bills_display_options = None
    sponsored_bill_keyword = None

    st.title('Congressional Member Info Data Tool')

    congress_sessions = ["118th Congress (2023-2025)",
                         "119th Congress (2025-2027)"]
    
    congress_session_selection = st.selectbox('Optional - Select a Congressional Session:',
                                    congress_sessions,
                                    index=None
                                    )
    # Limit datasets to selected congress session or any session in congress_sessions list
    if congress_session_selection:
        congress_session = int(congress_session_selection[:3])
        member_options = member_options[member_options['congress'] == congress_session]
        voting_results = voting_results[voting_results['congress'] == congress_session]
        sponsored_bills = sponsored_bills[sponsored_bills['congress'] == congress_session]
    else:
        congress_sessions_int = [int(session[:3]) for session in congress_sessions]
        member_options = member_options[member_options['congress'].isin(congress_sessions_int)]
        voting_results = voting_results[voting_results['congress'].isin(congress_sessions_int)]
        sponsored_bills = sponsored_bills[sponsored_bills['congress'].isin(congress_sessions_int)]
    
    chambers = ['Senate', 'House of Representatives']
    selected_chamber = st.selectbox('Select a chamber of Congress:', 
                                    chambers, 
                                    index=None)

    if selected_chamber:
        selected_method = st.selectbox('Select a search method:', ['Search Member by Zip Code', 'Search Member by Name'],
                                       index=None)

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
    
    if selected_method == 'Search Member by Zip Code':

        # Step 1: Enter zip code or representative
        
        zip_code = st.text_input('Enter your zip code:')
    
        if zip_code:
            try:                
                district_states = dim_congressional_districts[dim_congressional_districts['zip_code'] == int(zip_code)]['state'].values

                if selected_chamber == 'House of Representatives':
                    congressional_districts = dim_congressional_districts[dim_congressional_districts['zip_code'] == int(zip_code)]['congressional_district'].values
                    district_states = dim_congressional_districts[dim_congressional_districts['zip_code'] == int(zip_code)]['state'].values
                    options_df = member_options[
                                                (member_options['chamber'] == selected_chamber) & \
                                                # NA filled with 0 because states with only 1 congressional district have value of NA in this view
                                                (member_options['congressional_district'].fillna(0).isin(congressional_districts)) & \
                                                (member_options['stateName'].isin(district_states))
                                                ]
                    
                elif selected_chamber == 'Senate':
                    options_df = member_options[
                                                (member_options['chamber'] == selected_chamber) & \
                                                (member_options['stateName'].isin(district_states))
                                                ]

            except Exception as e:
                st.write('An error occurred. Please ensure that your entry is valid.')
                st.error(e)
                st.stop()
            if options_df.empty:
                st.write('No representative found for this entry. Please ensure that your entry is valid.')
    
    
    if selected_method == 'Search Member by Name':

        last_name = st.text_input("Enter your representative's last name:")

        if last_name:
        
            try:
                options_df = member_options[
                                                (member_options['chamber'] == selected_chamber) & \
                                                (member_options['lastName'] == last_name)
                                                ]
            except Exception as e:
                st.write('An error occurred. Please ensure that your entry is valid.')
                st.stop()
            
            if options_df.empty:
                st.write('No representative found for this entry. Please ensure that your entry is valid.')
    
    
    if options_df is not None:
        options = options_df['concat_name'].unique().tolist()
        selected_option = st.selectbox('Select a representative:', 
                                    options,
                                    index=None)
    if selected_option:
            # st.write('Collapse sidebar for full view after selecting member.')
            st.markdown("""
                        <style>
                        .small-font {
                            font-size:12px !important;
                            font-style: italic;
                        }
                        </style>
                        """, unsafe_allow_html=True)

            st.markdown('<div class="small-font">Collapse sidebar for full view after selecting member.</div>', unsafe_allow_html=True)
            st.write("")
            rep_name = selected_option.split(' | ')[0] 
            try:
                # additional_data_df = query_by_name(rep_name)
                additional_data_df = dim_members[dim_members['invertedOrderName'] == rep_name]
            except Exception as e:
                st.write('An error occurred.')
            
            chamber = additional_data_df.iloc[0]['mostRecentChamber']
            bioguideID = additional_data_df.iloc[0]['bioguideID']
            partyName = additional_data_df.iloc[0]['mostRecentParty']
            policy_areas_voting_record = voting_results\
                                        [(voting_results.bioguideID == bioguideID) & (voting_results.policyArea.isna() == False)]\
                                        ['policyArea'].\
                                        value_counts().sort_index()
            policy_areas_sponsored_bills = sponsored_bills\
                                           [(sponsored_bills.bioguideID == bioguideID) & (sponsored_bills.policyArea.isna() == False)]\
                                           ['policyArea'].\
                                           value_counts().sort_index()
    
    
    

    st.markdown(f"***Data last updated {last_updated} PST***")
    st.markdown("**Find the full project [here](https://github.com/Lbongard/congress_pipeline)**")
    
    
            

with tab1:
    tab1_col = st.columns((6.25, 1.75))
    with tab1_col[0]:       
        if selected_option:
            
            formatted_policy_list_votes = [f"{item} | (n={count})" for item, count in policy_areas_voting_record.items()]

            policy_area_selection_voting_record = st.selectbox("Optional - Filter results by bill subject", 
                                        formatted_policy_list_votes,
                                        index=None,
                                        key='policy_area_selection_voting_record')
            
            def reset_selection():
                st.session_state.policy_area_selection_voting_record = None

            # st.button("Clear Policy Area Selection", on_click=reset_selection)
            
            if policy_area_selection_voting_record:
                policy_area_voting_record = policy_area_selection_voting_record.split(' | ')[0]
            
            fig = plot_voting_records(results=voting_results, 
                                    bioguideID=bioguideID, 
                                    chamber=chamber,
                                    policy_area=policy_area_voting_record
                                    )
            st.plotly_chart(fig, use_container_width=False, config = {'displayModeBar': False})
            



    with tab1_col[1]:
        if additional_data_df is not None:

            display_term_summary(additional_data_df=additional_data_df, 
                                 terms_df=terms_df, 
                                 mem_name=selected_option, 
                                 bioguideID=bioguideID)

with tab2:
    tab2_col = st.columns((6.25, 1.75))
    
    with tab2_col[0]:
        if selected_option:
            indiv_vote_display_options = st.radio("Options:", ['See Recent Votes', 'Search Votes by Bill Keyword'])

            if indiv_vote_display_options == 'Search Votes by Bill Keyword':
                bill_keyword_indiv_votes = st.text_input("Enter a Bill Keyword:")
                keyword_filter_indiv_votes = voting_results['title'].str.lower().str.contains(bill_keyword_indiv_votes.lower())
            else:
                bill_keyword_indiv_votes = None
                keyword_filter_indiv_votes = True

            policy_area_selection_indiv_votes = st.selectbox("Optional - Filter results by bill subject", 
                            formatted_policy_list_votes,
                            index=None,
                            key='policy_area_selection_indiv_votes')
            
            if policy_area_selection_indiv_votes:
                policy_area_selection_indiv_votes = policy_area_selection_indiv_votes.split(' | ')[0]
                policy_area_filter_indiv_votes = voting_results['policyArea'] == policy_area_selection_indiv_votes
            else:
                policy_area_filter_indiv_votes = True

                
            # Display table of recent votes
            display_cols_indiv_votes = [
                'vote_date', 'question', 'title', 'url', 'roll_call_number', 'policyArea', 'member_vote', 'dem_majority_vote', 'rep_majority_vote', 'result', 'partyName'
                ]
                       

            vote_cell_styles = {}

            for col in ['member_vote', 'dem_majority_vote', 'rep_majority_vote']:
                 yea_aye_condition     =  f"params.data.{col} === 'Yea' || params.data.{col} === 'Aye'"
                 nay_no_condition      =  f"params.data.{col} === 'Nay' || params.data.{col} === 'No'"

                 vote_cell_styles[col] = \
                 highlight_mult_colors(primary_condition=yea_aye_condition,
                                       primary_color="#abf7b1", # Light Green
                                       secondary_condition=nay_no_condition,
                                       secondary_color="#fcccbb", # Light Red
                                       final_color=None,
                                       font_size=12
                                      )
            
            formatter_indiv_votes = {
            'title': ('Title (Click for more info)', {'width': 200, 'wrapText': True, 'autoHeight': True, 'cellRenderer':cellRenderer, 'cellStyle': {'font-size': '12px'}}),
            'question': ('Question', {'width': 125,'wrapText': True, 'autoHeight': True, 'cellStyle': {'font-size': '12px'}}),
            # 'url': ('Link', {'cellRenderer':cellRenderer}),
            'member_vote': ('Member Vote', {'width': 115, 'autoHeight': True, 'cellStyle':vote_cell_styles['member_vote']}),
            'dem_majority_vote': ('Dem Maj Vote', {'width': 115, 'autoHeight': True, 'cellStyle':vote_cell_styles['dem_majority_vote']}),
            'rep_majority_vote': ('Rep Maj Vote', {'width': 115, 'autoHeight': True, 'cellStyle':vote_cell_styles['rep_majority_vote']}),
            'result': ('Result', {'width': 125, 'cellStyle': {'font-size': '12px'}}),
            'vote_date': ('Vote Date', {'width': 125, 'cellStyle': {'font-size': '12px'}, 'valueFormatter':"(new Date(value)).toISOString().split('T')[0]"}),
            # 'policyArea': ('Policy Area', {'width': 150}),
            'roll_call_number': ('Vote #', {'width': 110, 'cellStyle': {'font-size': '12px'}})
            }
            
            voting_results_table_data = voting_results[(voting_results['bioguideID'] == bioguideID) & \
                                                       (keyword_filter_indiv_votes) & \
                                                       (policy_area_filter_indiv_votes)]\
                                        [display_cols_indiv_votes].\
                                        sort_values('vote_date', ascending=False)
            

            #### PAGINATION LOGIC STARTS HERE

            # Code adapted from Carlos Serrano: https://medium.com/streamlit/paginating-dataframes-with-streamlit-2da29b080920

            st.markdown("---")
            
            
            page_selection_menu_votes = st.columns((4, 1, 1))

            with page_selection_menu_votes[0]:
                st.markdown("<h3><b>Recent Voting Results</b> ✅</h3>", unsafe_allow_html=True)
            with page_selection_menu_votes[2]:
                batch_size_votes = st.selectbox("Page Size", options=[25, 50, 100], key='voting_results_size')
            with page_selection_menu_votes[1]:
                total_pages_votes = (
                    int(len(voting_results_table_data) / batch_size_votes) if int(len(voting_results_table_data) / batch_size_votes) > 0 else 1
                )
                current_page_votes = st.number_input(
                    "Page", min_value=1, max_value=total_pages_votes, step=1, key='voting_results_page'
                )
           
            pagination_voting_results = st.container()

            bottom_menu_votes = st.columns((4, 1, 1))

            with bottom_menu_votes[0]:
                st.markdown(f"Page **{current_page_votes}** of **{total_pages_votes}** ")


            #### Display Logic Continues Here
            with pagination_voting_results:
                # if indiv_vote_display_options == 'See Recent Votes':
                voting_results_table_data = voting_results_table_data.iloc[(current_page_votes-1)*batch_size_votes:(current_page_votes-1)*batch_size_votes+(batch_size_votes+1), :]

                if voting_results_table_data.empty:
                    st.text("No Votes to Display")
                else:
                    data = draw_grid(
                        voting_results_table_data,
                        formatter=formatter_indiv_votes,
                        selection='single', 
                        wrap_text=True,
                        grid_options={'domLayout':'normal',
                                    'enableCellTextSelection':True}
                    )

                    st.markdown("""
                                <style>
                                .ag-theme-streamlit {
                                    padding-bottom: 20px;  /* Extra space for bottom content */
                                }
                                </style>
                            """, unsafe_allow_html=True)

    with tab2_col[1]:
        if additional_data_df is not None:
            display_term_summary(additional_data_df=additional_data_df, 
                                 terms_df=terms_df, 
                                 mem_name=selected_option, 
                                 bioguideID=bioguideID)
        
with tab3:
    tab3_col = st.columns((6.25, 1.75))
    with tab3_col[0]:
        if selected_option:
            sponsored_bills_display_options = st.radio("Options:", ['See Recent Sponsored Bills', 'Search Sponsored Bills by Keyword'])

            formatted_policy_list_votes = [f"{item} | (n={count})" for item, count in policy_areas_sponsored_bills.items()]

            policy_area_selection_sponsored_bills = st.selectbox("Optional - Filter results by bill subject", 
                                                                 formatted_policy_list_votes,
                                                                 index=None,
                                                                 key='policy_area_selection_sponsored_bills')
            
            if policy_area_selection_sponsored_bills:
                policy_area_selection_sponsored_bills = policy_area_selection_sponsored_bills.split(' | ')[0]
                policy_area_filter_sponsored_bills = (sponsored_bills['policyArea'] == policy_area_selection_sponsored_bills)
            else:
                policy_area_filter_sponsored_bills = True
            
            if sponsored_bills_display_options == 'Search Sponsored Bills by Keyword':
                sponsored_bill_keyword = st.text_input("Enter a Bill Keyword:")
                keyword_filter_sponsored_bills = sponsored_bills['title'].str.lower().str.contains(sponsored_bill_keyword.lower())
            else:
                sponsored_bill_keyword = None
                keyword_filter_sponsored_bills = True

            display_cols_sponsored_bills = ['title', 'url', 'sponsor_type', 'policyArea', 'introducedDate']
            
            sponsored_bills_table_data = sponsored_bills[(sponsored_bills['bioguideID'] == bioguideID) & \
                                                       (keyword_filter_sponsored_bills) & \
                                                       (policy_area_filter_sponsored_bills)]\
                                                        [display_cols_sponsored_bills].\
                                                        sort_values('introducedDate', ascending=False).\
                                                        fillna({'policyArea':'-'})
                                                        # set_index('introducedDate').\
                                                        # sort_index(ascending = False)
            

            formatter_sponsored_bills = {
            'title': ('Title (Click for more info)', {'width': 250, 'wrapText': True, 'autoHeight': True, 'cellRenderer':cellRenderer, 'cellStyle': {'font-size': '12px'}}),
            # 'url': ('Link', {'cellRenderer':cellRenderer}),
            'sponsor_type': ('Sponsor Type', {'width': 200, 'cellStyle': {'font-size': '12px'}}),
            'policyArea': ('Policy Area', {'width': 200, 'cellStyle': {'font-size': '12px'}}),
            'introducedDate': ('Date Introduced', {'width': 150, 'cellStyle': {'font-size': '12px'}, 'autoHeight': True}),
            }
            
            #### Pagination logic starts here
            st.markdown("---")
            
            
            page_selection_menu_bills = st.columns((4, 1, 1))

            with page_selection_menu_bills[0]:
                st.markdown("<h3><b>Recent Sponsored Legislation</b> ✍️ </h3>", unsafe_allow_html=True)
            with page_selection_menu_bills[2]:
                batch_size_bills = st.selectbox("Page Size", options=[25, 50, 100], key='sponsored_bills_size')
            with page_selection_menu_bills[1]:
                total_pages_bills = (
                    int(len(sponsored_bills_table_data) / batch_size_bills) if int(len(sponsored_bills_table_data) / batch_size_bills) > 0 else 1
                )
                current_page_bills = st.number_input(
                    "Page", min_value=1, max_value=total_pages_bills, step=1, key='sponsored_bills_page'
                )
           
            pagination_sponsored_bills = st.container()

            bottom_menu_bills = st.columns((4, 1, 1))

            with bottom_menu_bills[0]:
                st.markdown(f"Page **{current_page_bills}** of **{total_pages_bills}** ")

        

            # sponsored_bills_table_data = sponsored_bills_table_data[['title']]
            with pagination_sponsored_bills:
                if sponsored_bills_display_options == 'See Recent Sponsored Bills':
                    sponsored_bills_table_data = sponsored_bills_table_data.iloc[(current_page_bills-1)*batch_size_bills:(current_page_bills-1)*batch_size_bills+(batch_size_bills+1),:]
                    

                if sponsored_bills_table_data.empty:
                    st.text('No sponsored legislation found for this member.')
                else:
                    data = draw_grid(
                        sponsored_bills_table_data,
                        formatter=formatter_sponsored_bills,
                        # fit_columns=True,
                        selection='single', 
                        # max_height=300,
                        wrap_text=True,
                        # auto_height=True,
                        grid_options={'domLayout':'normal',
                                    'enableCellTextSelection':True,
                                     "getRowHeight": "function(params) { return Math.max(32, params.data.row_height || 32); }"}

                    )
    with tab3_col[1]:
        if additional_data_df is not None:
            display_term_summary(additional_data_df=additional_data_df, 
                                 terms_df=terms_df, 
                                 mem_name=selected_option, 
                                 bioguideID=bioguideID)

