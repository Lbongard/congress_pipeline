{{
    config(
        materialized='table'
    )
}}
select  
        {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
        roll_call_number,
        concat(bill_type,bill_number) bill_name,
        bioguideID,
        vote

from {{ref("stg_votes")}}