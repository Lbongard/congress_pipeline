{{
    config(
        materialized='table'
    )
}}

with members_staged as(

  select distinct * except(depiction)
  from {{ref("stg_members")}})


SELECT bioguideID
       ,party.partyName
       ,party.startYear
       ,party.endYear
       ,CASE 
            WHEN party.partyName in ('Democratic', 'Republican') THEN party.partyName
            ELSE 'Independent'
            END partyGrouped
FROM members_staged
,UNNEST(partyHistory) party