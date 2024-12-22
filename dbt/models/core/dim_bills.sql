{{
    config(
        materialized='incremental',
        unique_key=['bill_key'],
        incremental_strategy='merge',
        merge_exclude_columns=['flowDate']
    )
}}

with source as(
    select *
    from {{ref("stg_bills")}}
)

SELECT bill_key
      ,concat(type, number) bill_name
      ,number
      ,type
      ,title
      ,CAST(congress AS INT64) congress
      -- Create link to bill info
       ,concat('https://www.congress.gov/bill/',
         CAST(congress AS STRING),
         -- function to get ordinal number from https://stackoverflow.com/questions/30172995/how-to-create-ordinal-numbers-i-e-1st-2nd-etc-in-sql-server
         CASE
          WHEN MOD(CAST(congress AS INT64), 100) IN (11, 12, 13) THEN 'th' -- First checks for exception
          WHEN MOD(CAST(congress AS INT64), 10) = 1 THEN 'st'
          WHEN MOD(CAST(congress AS INT64), 10) = 2 THEN 'nd'
          WHEN MOD(CAST(congress AS INT64), 10) = 3 THEN 'rd'
          ELSE 'th' -- Works for MOD(congress, 10) IN (4,5,6,7,8,9,0)
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
            )as url
        ,JSON_VALUE(policyArea, '$.name') policyArea
        ,updateDateIncludingText
        ,CAST(introducedDate AS DATE) introducedDate 
        ,current_datetime() as flowDate
FROM source
QUALIFY ROW_NUMBER() OVER(PARTITION BY bill_key ORDER BY updateDateIncludingText) = 1