{{
    config(
        materialized='table',
        partition_by={
            "field": "introducedDate",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

SELECT sponsors.bioguideID
      ,mems.name
      ,sponsor_type
      ,bill_name
      ,title
      ,url
      ,policy_area
      ,introducedDate
FROM {{ref('dim_sponsors')}} sponsors join {{ref('dim_bills')}} bills on (sponsors.bill_key = bills.bill_key)
        join {{ref('dim_members')}} mems on (sponsors.bioguideID = mems.bioguideID)