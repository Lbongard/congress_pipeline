{{
    config(
        materialized='incremental',
        unique_key=['bill_key', 'committee_key', 'chamber'],
        incremental_strategy='merge',
        merge_exclude_columns=['updateDatetime']
    )
}}

with 
source as(
     select *
    from {{ref("stg_bills")}}
),

unnested_data as(
SELECT distinct 
            bill_key
            ,JSON_EXTRACT_SCALAR(committee, '$.chamber') chamber
            ,JSON_EXTRACT_SCALAR(committee, '$.name') name
FROM source,
UNNEST(JSON_EXTRACT_ARRAY(committees, '$.item')) AS committee
)

select *, 
       {{ dbt_utils.generate_surrogate_key(['name']) }} committee_key,
       current_datetime() as updateDatetime
from unnested_data