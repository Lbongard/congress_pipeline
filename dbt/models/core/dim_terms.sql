{{
    config(
        materialized='table'
    )
}}

with stg_terms as(
    SELECT distinct *
    FROM {{ref("stg_terms")}}
)

select * 
from stg_terms