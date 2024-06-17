with source as(
    select * from {{source('staging', 'members')}}
)

select s.bioguideID,
       item.startYear term_start_year,
       item.endYear term_end_year,
       item.chamber
from source s, 
     UNNEST(terms.item) as item


