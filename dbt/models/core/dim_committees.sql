{{
    config(
        materialized='table'
    )
}}

with 
source as(
    select * from {{ref("stg_bills")}}
)

select  distinct item.*
        from source,
        UNNEST(committees.item) item