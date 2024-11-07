with 

source as(
    select * 
    from {{source('staging', 'house_votes_External')}}
),

all_vote_data AS(
  SELECT rollcall_vote.*,
         bill_number,
         bill_type
  FROM source)

  SELECT vote_metadata.*,
         bill_number,
         bill_type
  FROM all_vote_data