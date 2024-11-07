with source as(
    select * from {{source('staging', 'senate_ids_External')}}
)

select JSON_VALUE(full_name, '$.first_name') first_name,
       JSON_VALUE(full_name, '$.last_name') last_name,
       state,
       bioguide bioguideID,
       lisid
    
from source
,UNNEST(lisid) lisid