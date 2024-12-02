{{
    config(
        materialized='incremental',
        unique_key=['bill_key', 'subject'],
        incremental_strategy='merge',
        merge_exclude_columns=['updateDatetime']
    )
}}

with 
source as(
    select * from {{ref("stg_bills")}}
)

SELECT bill_key,
       JSON_EXTRACT_SCALAR(leg_subjects, '$.name') subject,
       current_datetime() as updateDatetime
FROM Congress_Stg.stg_bills,
UNNEST(JSON_EXTRACT_ARRAY(subjects, '$.legislativeSubjects.item')) leg_subjects