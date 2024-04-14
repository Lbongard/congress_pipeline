with source as(
    select * from {{source('staging', 'member')}}
)

select s.bioguideID,
       s.state,
       s.partyName,
       s.district,
       s.depiction,
       s.depiction.imageURL imageURL,
       terms.chamber chamber,
    --    terms.startYear term_start_year,
    --    terms.endYear term_end_year
from source s


