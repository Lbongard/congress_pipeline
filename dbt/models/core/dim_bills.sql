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
       concat(type, number) bill_name,
       title,
       policyArea.name policy_area,
       latestAction.ActionDate latestActionDate,
       latestAction.text latestActionText
from source,
UNNEST(subjects.legislativeSubjects.item) item

