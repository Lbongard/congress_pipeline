with 

source as(
    select bill.* from {{source('staging', 'bill_status')}}
)

select *,
      {{ dbt_utils.generate_surrogate_key(['type', 'number'])}} as bill_key,
from source