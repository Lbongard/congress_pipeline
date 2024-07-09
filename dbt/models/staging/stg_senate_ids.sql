with source as(
    select * from {{source('staging', 'senate_ids')}}
)

select *
from source