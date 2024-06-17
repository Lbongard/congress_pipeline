{{
    config(
        materialized='table'
    )
}}
with source as(
    select *
    from {{ref("stg_bills")}}
)

select distinct bill_key,
       number,
       type,
       introducedDate,
       concat(type, number) bill_name,
       title,
       most_recent_text,
       policyArea.name policy_area,
       latestAction.ActionDate latestActionDate,
       latestAction.text latestActionText
from source

