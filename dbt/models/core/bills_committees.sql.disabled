{{
    config(
        materialized='table'
    )
}}

with bills_committees_unnested as (
    select bill_key,
           item.*
    from {{ref("stg_bills")}},
         UNNEST(committees.item) item
)

select distinct name committeeName,
                bill_key
from bills_committees_unnested