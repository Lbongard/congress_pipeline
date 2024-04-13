with 

source as(
    select * from {{source('staging', 'member')}}
)

select bioguideID,
       state,
       partyName,
       district
from source