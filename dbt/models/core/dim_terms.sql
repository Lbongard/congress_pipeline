{{
    config(
        materialized='incremental',
        unique_key=['bioguideID', 'term_start_year', 'chamber'],
        incremental_strategy='merge',
        merge_exclude_columns=['flowDate']
    )
}}

with stg_terms as(
    SELECT distinct *,
                    current_datetime() as flowDate
    FROM {{ref("stg_terms")}}
)

select * 
from stg_terms