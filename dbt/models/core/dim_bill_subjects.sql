with 
source as(
    select * from {{source('staging', 'bill_status')}}
)

select distinct bill.number,
                bill.type,
                item.*
from source,
UNNEST(bill.subjects.legislativeSubjects.item) item