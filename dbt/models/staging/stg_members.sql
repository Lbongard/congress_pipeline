

-- select s.bioguideID,
--        s.name,
--        s.state,
--        s.partyName,
--        s.district,
--        s.depiction,
--        s.depiction.imageURL imageURL
--     --    terms.startYear term_start_year,
--     --    terms.endYear term_end_year
-- from source s, 
--      UNNEST(terms.item) as item

with 

source as(
    select * from {{source('staging', 'members_External')}}
)

select *
from source

