{{
    config(
        materialized='table'
    )
}}
select  
        {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
        roll_call_number,
        chamber,
        concat(bill_type,bill_number) bill_name,
        bioguideID,
        vote,
        party

from {{ref("stg_votes")}}