{{
    config(
        materialized='incremental',
        unique_key=['bioguideID', 'startYear'],
        incremental_strategy='merge',
        merge_exclude_columns=['flowDate']
    )
}}
with members_staged as(

  select * except(depiction)
  from {{ref("stg_members")}})


SELECT distinct 
        bioguideID
       ,party.partyName
       ,party.startYear
       ,party.endYear
       ,CASE 
            WHEN party.partyName in ('Democratic', 'Republican') THEN party.partyName
            ELSE 'Independent'
            END partyGrouped
       ,current_datetime() as flowDate
FROM members_staged
,UNNEST(partyHistory) party