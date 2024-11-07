{{
    config(
        materialized='table'
    )
}}

with 
source as(
     select *
    from {{ref("stg_bills")}}
)

SELECT distinct 
            bill_key
            ,JSON_EXTRACT_SCALAR(committee, '$.chamber') chamber
            ,JSON_EXTRACT_SCALAR(committee, '$.name') name
            ,JSON_EXTRACT_SCALAR(committee, '$.systemCode') systemCode
FROM source,
UNNEST(JSON_EXTRACT_ARRAY(committees, '$.item')) AS committee