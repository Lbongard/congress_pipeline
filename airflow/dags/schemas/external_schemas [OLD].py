import os
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")

bill_status_ddl = f"""CREATE OR REPLACE EXTERNAL TABLE Congress.bill_status_external_table(
   bill STRUCT<
        number STRING,
        updateDate TIMESTAMP,
        type STRING,
        introducedDate DATE,
        congress INT64,
        committees STRUCT<
            item ARRAY<STRUCT<
                name STRING,
                chamber STRING,
                type STRING
            >>
        >,
        actions STRUCT<
            item ARRAY<STRUCT<
                actionDate DATE,
                text STRING,
                type STRING,
                actionCode STRING,
                recordedVotes ARRAY<STRUCT<
                    rollNumber STRING,
                    url STRING,
                    chamber STRING,
                    congress INT64,
                    date TIMESTAMP,
                    sessionNumber STRING
                >>
            >>
        >,
        sponsors STRUCT<
            item ARRAY<STRUCT<
                bioguideID STRING,
                fulName STRING,
                firstName STRING,
                lastName STRING
            >>
        >,
        cosponsors STRUCT<
            count INT64,
            item ARRAY<STRUCT<
                bioguideID STRING
            >>
        >,
        policyArea STRUCT<
            name STRING
        >,
        subjects STRUCT<
            legislativeSubjects STRUCT<
                item ARRAY<STRUCT<
                    name STRING
                >>
            >
        >,
        title STRING,
        latestAction STRUCT<
            actionDate DATE,
            text STRING
        >,
        most_recent_text STRING
    >
)

OPTIONS (
  format = 'JSON',
  uris = [
    'gs://{BUCKET_NAME}/bill_status/117/hconres/*.json',
    'gs://{BUCKET_NAME}/bill_status/117/hjres/*.json',
    'gs://{BUCKET_NAME}/bill_status/117/hr/*.json',
    'gs://{BUCKET_NAME}/bill_status/117/hres/*.json',
    'gs://{BUCKET_NAME}/bill_status/117/s/*.json',
    'gs://{BUCKET_NAME}/bill_status/117/sjres/*.json',
    'gs://{BUCKET_NAME}/bill_status/117/sres/*.json',
    'gs://{BUCKET_NAME}/bill_status/118/hconres/*.json',
    'gs://{BUCKET_NAME}/bill_status/118/hjres/*.json',
    'gs://{BUCKET_NAME}/bill_status/118/hr/*.json',
    'gs://{BUCKET_NAME}/bill_status/118/hres/*.json',
    'gs://{BUCKET_NAME}/bill_status/118/s/*.json',
    'gs://{BUCKET_NAME}/bill_status/118/sjres/*.json',
    'gs://{BUCKET_NAME}/bill_status/118/sres/*.json'
  ]);

"""

vote_ddl = f"""CREATE OR REPLACE EXTERNAL TABLE Congress.votes_external_table (
  vote STRUCT<
    date DATE,
    congress INTEGER,
    session STRING,
    bill_type STRING,
    bill_number STRING,
    chamber STRING,
    roll_call_number STRING,
    result STRING,
    totals STRUCT<
      yea INTEGER,
      nay INTEGER,
      present INTEGER,
      not_voting INTEGER
    >
  >,
  legislator ARRAY<STRUCT<
    id STRING,
    first_name STRING,
    last_name STRING,
    party STRING,
    state STRING,
    vote STRING
  >>
)
OPTIONS (
  format = "JSON",
  uris = ['gs://{BUCKET_NAME}/votes/117/hconres/*.json',
    'gs://{BUCKET_NAME}/votes/117/hjres/*.json',
    'gs://{BUCKET_NAME}/votes/117/hr/*.json',
    'gs://{BUCKET_NAME}/votes/117/hres/*.json',
    'gs://{BUCKET_NAME}/votes/117/s/*.json',
    'gs://{BUCKET_NAME}/votes/117/sjres/*.json',
    'gs://{BUCKET_NAME}/votes/117/sres/*.json',
    'gs://{BUCKET_NAME}/votes/118/sconres/*.json',
    'gs://{BUCKET_NAME}/votes/118/hjres/*.json',
    'gs://{BUCKET_NAME}/votes/118/hr/*.json',
    'gs://{BUCKET_NAME}/votes/118/hres/*.json',
    'gs://{BUCKET_NAME}/votes/118/s/*.json',
    'gs://{BUCKET_NAME}/votes/118/sjres/*.json',
    'gs://{BUCKET_NAME}/votes/118/sres/*.json'
    ]
)"""

member_ddl = f"""CREATE OR REPLACE EXTERNAL TABLE Congress.members_external_table (
  bioguideId STRING,
  state STRING,
  partyName STRING,
  name STRING,
  terms STRUCT<
  item ARRAY<STRUCT<chamber STRING, startYear INT64, endYear INT64>>
        >,
  updateDate STRING,
  url STRING,
  district FLOAT64,
  depiction STRUCT<attribution STRING, 
                   imageURL STRING>,
  depiction_key STRING,
  depiction_value STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://{BUCKET_NAME}/members/members*.json']
  )
"""

senate_id_ddl = f"""CREATE OR REPLACE EXTERNAL TABLE Congress.senate_ids_external_table (
  first_name STRING,
  last_name STRING,
  state STRING,
  bioguideID STRING,
  lisid STRING
)
OPTIONS(
  format = 'JSON',
  uris = ['gs://{BUCKET_NAME}/senate_ids/*.json']
  )
"""