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
        ,item.*
        from source,
        UNNEST(committees.item) item