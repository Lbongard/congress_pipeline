{{
    config(
        materialized='table'
    )
}}

with house_vote_data_nested as(
  SELECT 
        {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
         vote_metadata.*,
         concat(bill_type, bill_number) bill_name,
         vote_data.*
  FROM {{ref("stg_house_votes")}}
),

house_vote_data as(

SELECT 
       bill_key,
       CAST(rollcall_num AS INT) roll_call_number,
       CAST(congress AS INT) congress,
       CAST(LEFT(session, 1) AS INT) session,
       REPLACE(chamber, 'U.S. ', '') chamber,
       bill_name,
       legislator_votes.legislator.name_id bioguideID,
       CAST(NULL AS STRING) as lisid,
       legislator_votes.vote,
       legislator_votes.legislator.party,
FROM house_vote_data_nested,
UNNEST(recorded_vote) legislator_votes
),

senate_vote_data_nested as(
  SELECT 
         {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
         roll_call_vote.*,
         chamber,
         concat(bill_type, bill_number) bill_name,
         roll_call_vote.members.*
  FROM {{ref("stg_sen_votes")}}
),

senate_vote_data as(
SELECT   
         bill_key,
         CAST(vote_number AS INT) roll_call_number,
         CAST(congress AS INT),
         CAST(session AS INT),
         chamber,
         bill_name,
         CAST(NULL AS STRING) as bioguideID,
         member.lis_member_id as lisid,
         member.vote_cast vote,
         member.party,
FROM senate_vote_data_nested,
UNNEST(member) member
)

SELECT *
FROM house_vote_data

UNION ALL

SELECT *
FROM senate_vote_data
