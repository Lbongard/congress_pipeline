{{
    config(
        materialized='table'
    )
}}

with 
source as(
    select bills.bill_key
           ,bills.bill_name
           ,committees.name
           ,committees.chamber
    from {{ref("dim_bills")}} bills join {{ref("dim_committees")}} committees
        on (bills.bill_key = committees.bill_key)

)

select *
from source