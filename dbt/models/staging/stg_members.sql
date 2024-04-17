with source as(
    select * from {{source('staging', 'members')}}
)

select s.bioguideID,
       s.state,
       s.partyName,
       s.district,
       s.depiction,
       s.depiction.imageURL imageURL,
       item.chamber chamber,
    --    terms.startYear term_start_year,
    --    terms.endYear term_end_year
from source s, 
     UNNEST(terms.item) as item


