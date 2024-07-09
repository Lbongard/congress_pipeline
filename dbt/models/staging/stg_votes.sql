with 

source as(
    select * 
    from {{source('staging', 'votes')}}
)

select distinct s.vote.date,
       s.vote.bill_type,
       s.vote.bill_number,
       s.vote.chamber,
       s.vote.roll_call_number,
       s.vote.result,
       s.vote.totals.yea yea_totals,
       s.vote.totals.nay nay_totals,
       s.vote.totals.present present_totals, 
       s.vote.totals.not_voting not_voting_totals,
       legislator.id id,
       legislator.vote,
       legislator.party,
       concat(legislator.last_name, ", ", legislator.first_name) legislator_name
from source s, 
     UNNEST(legislator) as legislator