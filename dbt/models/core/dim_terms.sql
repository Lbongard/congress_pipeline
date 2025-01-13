{{
    config(
        materialized='incremental',
        unique_key=['bioguideID', 'term_start_year', 'chamber'],
        incremental_strategy='merge',
        merge_exclude_columns=['flowDate']
    )
}}

with stg_terms as(
    SELECT *, current_date() as flowDate
FROM {{ref("stg_terms")}}
QUALIFY ROW_NUMBER() OVER(PARTITION BY bioguideId, term_start_year, chamber ORDER BY COALESCE(term_end_year, 9999) desc) = 1
)

select * 
from stg_terms