{{
    config(
        materialized='incremental',
        unique_key=['committee_key', 'chamber'],
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
SELECT 
        DISTINCT 
            JSON_EXTRACT_SCALAR(committee, '$.chamber') chamber
            ,JSON_EXTRACT_SCALAR(committee, '$.name') name
            ,JSON_EXTRACT_SCALAR(committee, '$.systemCode') systemCode
FROM source,
UNNEST(JSON_EXTRACT_ARRAY(committees, '$.item')) AS committee
)

SELECT *, 
       {{ dbt_utils.generate_surrogate_key(['name']) }} committee_key,
       current_datetime() as updateDatetime
FROM unnested_data