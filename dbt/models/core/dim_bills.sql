{{
    config(
        materialized='table'
    )
}}
with source as(
    select *
    from {{ref("stg_bills")}}
)

-- select distinct bill_key,
--        number,
--        type,
--        introducedDate,
--        concat(type, number) bill_name,
--        congress,
--        title,
--        most_recent_text,
--        policyArea.name policy_area,
--        latestAction.ActionDate latestActionDate,
--        latestAction.text latestActionText,
--        -- Create link to bill info
--        concat('https://www.congress.gov/bill/',
--          CAST(congress AS STRING),
--          -- function to get ordinal number from https://stackoverflow.com/questions/30172995/how-to-create-ordinal-numbers-i-e-1st-2nd-etc-in-sql-server
--          CASE
--           WHEN MOD(congress, 100) IN (11, 12, 13) THEN 'th' -- First checks for exception
--           WHEN MOD(congress, 10) = 1 THEN 'st'
--           WHEN MOD(congress, 10) = 2 THEN 'nd'
--           WHEN MOD(congress, 10) = 3 THEN 'rd'
--           ELSE 'th' -- Works for MOD(congress, 10) IN (4,5,6,7,8,9,0)
--           END,
--           '-congress/',
--           CASE type
--             WHEN 'S' THEN 'senate-bill'
--             WHEN 'HR' THEN 'house-bill'
--             WHEN 'SRES' THEN 'senate-resolution'
--             WHEN 'HRES' THEN 'house-resolution'
--             WHEN 'SJES' THEN 'senate-joint-resolution'
--             WHEN 'HJES' THEN 'house-joint-resolution'
--             WHEN 'HCONRES' THEN 'house-concurrent-resolution'
--             END,
--             '/',
--           number
--             ) as url

-- from source

SELECT distinct bill_key
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
            WHEN 'SJES' THEN 'senate-joint-resolution'
            WHEN 'HJES' THEN 'house-joint-resolution'
            WHEN 'HCONRES' THEN 'house-concurrent-resolution'
            END,
            '/',
            number
            )as url
        ,JSON_VALUE(policyArea, '$.name') policyArea
        ,updateDateIncludingText

FROM source
