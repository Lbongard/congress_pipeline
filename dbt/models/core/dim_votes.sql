{{
    config(
        materialized='table'
    )
}}
select distinct {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
       roll_call_number,
       date vote_date,
       bill_type,
       chamber,
       concat(bill_type, bill_number) bill_name,
       result,
       yea_totals,
       nay_totals,
       abs(yea_totals - nay_totals) yea_nay_gap_votes,
       present_totals,
       not_voting_totals

from {{ref("stg_votes")}}