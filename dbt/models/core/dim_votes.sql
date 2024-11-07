{{
    config(
        materialized='table'
    )
}}


SELECT * 
FROM {{ref("dim_house_votes")}}

UNION DISTINCT

SELECT * 
FROM {{ref("dim_senate_votes")}}