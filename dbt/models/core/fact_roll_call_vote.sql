{{
    config(
        materialized='table'
    )
}}
select  
        {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
        roll_call_number,
        replace(chamber, 'House', 'House of Representatives') chamber,
        concat(bill_type,bill_number) bill_name,
        id as bioguideID,
        null as lisid,
        vote,
        party,
        legislator_name
from {{ref("stg_votes")}}
where chamber = 'House'

UNION all
select  
        {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
        roll_call_number,
        chamber,
        concat(bill_type,bill_number) bill_name,
        null as bioguideID,
        id as lisid,
        vote,
        party,
        legislator_name
from {{ref("stg_votes")}}
where chamber = 'Senate'