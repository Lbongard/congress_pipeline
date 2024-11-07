
with 

source as(
    select * from {{source('staging', 'senate_votes_External')}}
)

select *
from source

