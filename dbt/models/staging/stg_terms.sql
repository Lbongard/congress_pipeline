with source as(
    select * from {{source('staging', 'members_External')}}
)

select bioguideId
       ,terms_list.* EXCEPT(startYear, endYear)
       ,terms_list.startYear term_start_year
       ,terms_list.endYear term_end_year
from source
,UNNEST(terms) terms_list
