{{
    config(
        materialized='incremental',
        unique_key=['congress', 'bill_key', 'roll_call_number', 'chamber'],
        incremental_strategy='merge',
        merge_exclude_columns=['flowDate']
    )
}}


SELECT * 
FROM {{ref("dim_house_votes")}}

UNION DISTINCT

SELECT * 
FROM {{ref("dim_senate_votes")}}