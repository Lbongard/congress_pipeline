{{
    config(
        materialized='table'
    )
}}

select zones.zcta zip_code,
       abbreviations.Name state,
       zones.cd congressional_district
from {{ref('congressional_zones')}} zones join {{ref('us-states-abbreviations')}} abbreviations 
     on (zones.state_abbr = trim(abbreviations.Abbreviation))