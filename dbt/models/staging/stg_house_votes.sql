with 

source as(
    select * 
    from {{source('staging', 'house_votes_External')}}
)

  SELECT rollcall_vote.*,
         bill_number,
         bill_type
  FROM source