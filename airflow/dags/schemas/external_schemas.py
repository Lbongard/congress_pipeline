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
        >
    >
)

OPTIONS (
  format = 'JSON',
  uris = [
    'gs://{BUCKET_NAME}/bill_status/hconres/*.json',
    'gs://{BUCKET_NAME}/bill_status/hjres/*.json',
    'gs://{BUCKET_NAME}/bill_status/hr/*.json',
    'gs://{BUCKET_NAME}/bill_status/s/*.json',
    'gs://{BUCKET_NAME}/bill_status/sjres/*.json',
    'gs://{BUCKET_NAME}/bill_status/sres/*.json'
  ]);

"""

vote_ddl = """CREATE OR REPLACE EXTERNAL TABLE Congress.votes_external_table (
  vote STRUCT<
    date DATE,
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
  uris = ['gs://{{BUCKET_NAME}}votes/HCONRES/*.json',
    'gs://{BUCKET_NAME}/votes/HJRES/*.json',
    'gs://{BUCKET_NAME}/votes/HR/*.json',
    'gs://{BUCKET_NAME}/votes/S/*.json',
    'gs://{BUCKET_NAME}/votes/SJRES/*.json',
    'gs://{BUCKET_NAME}/votes/SRES/*.json']
)"""

member_ddl = """CREATE OR REPLACE EXTERNAL TABLE Congress.members_external_table (
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
  uris = ['gs://{BUCKET_NAME}/members/*.json']
  )
"""