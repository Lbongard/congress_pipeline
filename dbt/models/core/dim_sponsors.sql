{{
    config(
        materialized='table'
    )
}}

with 
source as(
    select * from {{ref("stg_bills")}}
)

select  distinct bill_key
        ,item.bioguideID
        ,'Sponsor' as sponsor_type
        from source,
        UNNEST(sponsors.item) item

UNION ALL

SELECT distinct bill_key
        ,item.bioguideID
        ,'Coponsor' as sponsor_type
        from source,
        UNNEST(cosponsors.item) item
