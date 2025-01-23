{{ config(
    materialized='incremental',
    unique_key=['bill_key'],
    incremental_strategy='merge',
    merge_exclude_columns=['flowDate']
) }}

WITH source AS (
    SELECT *
    FROM {{ ref("stg_bills") }}
),

deduplicated AS (
    SELECT 
        bill_key,
        CONCAT(type, number) AS bill_name,
        number,
        type,
        title,
        CAST(congress AS INT64) AS congress,
        CONCAT(
            'https://www.congress.gov/bill/',
            CAST(congress AS STRING),
            CASE
                WHEN MOD(CAST(congress AS INT64), 100) IN (11, 12, 13) THEN 'th'
                WHEN MOD(CAST(congress AS INT64), 10) = 1 THEN 'st'
                WHEN MOD(CAST(congress AS INT64), 10) = 2 THEN 'nd'
                WHEN MOD(CAST(congress AS INT64), 10) = 3 THEN 'rd'
                ELSE 'th'
            END,
            '-congress/',
            CASE type
                WHEN 'S' THEN 'senate-bill'
                WHEN 'HR' THEN 'house-bill'
                WHEN 'SRES' THEN 'senate-resolution'
                WHEN 'HRES' THEN 'house-resolution'
                WHEN 'SJRES' THEN 'senate-joint-resolution'
                WHEN 'SCONRES' THEN 'senate-concurrent-resolution'
                WHEN 'HJRES' THEN 'house-joint-resolution'
                WHEN 'HCONRES' THEN 'house-concurrent-resolution'
            END,
            '/',
            number
        ) AS url,
        JSON_VALUE(policyArea, '$.name') AS policyArea,
        updateDateIncludingText,
        CAST(introducedDate AS DATE) AS introducedDate,
        CURRENT_DATETIME() AS flowDate
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bill_key ORDER BY updateDateIncludingText DESC) = 1
)

SELECT *
FROM deduplicated
