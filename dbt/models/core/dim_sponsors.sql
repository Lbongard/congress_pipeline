{{
    config(
        materialized='incremental',
        unique_key=['bill_key', 'bioguideID'],
        incremental_strategy='merge',
        merge_exclude_columns=['flowDate']
    )
}}

with 
source as(
    select * from {{ref("stg_bills")}}
)

SELECT  distinct bill_key,
        json_extract_scalar(sponsors_item, '$.bioguideId') bioguideID,
       'Sponsor' as sponsor_type,
       current_datetime() as flowDate
FROM `Congress_Stg.stg_bills`
,UNNEST(JSON_EXTRACT_ARRAY(sponsors, '$.item')) AS sponsors_item

UNION ALL

SELECT  distinct bill_key,
        json_extract_scalar(cosponsors_item, '$.bioguideId') bioguideID,
       'Cosponsor' as sponsor_type,
       current_datetime() as flowDate
FROM `Congress_Stg.stg_bills`
,UNNEST(JSON_EXTRACT_ARRAY(cosponsors, '$.item')) AS cosponsors_item
