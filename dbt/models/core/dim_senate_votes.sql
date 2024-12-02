{{
    config(
        materialized='incremental',
        unique_key=['congress', 'bill_key', 'roll_call_number'],
        incremental_strategy='merge',
        merge_exclude_columns=['flowDate']
    )
}}

WITH source AS(
  SELECT *
  FROM {{ref("stg_sen_votes")}}),


sen_votes_categorized as(
SELECT distinct 
          roll_call_vote.* EXCEPT(members)
          , bill_number
          , bill_type
          , concat(bill_type, bill_number) bill_name
          , {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key
          , member_unnested.party
          , member_unnested.lis_member_id
       --    , count.yeas
          ,  case 
                 WHEN vote_cast IN ('Yea', 'Aye') THEN 1 
                 ELSE 0
                 END yea_or_aye_vote
          , case  
                 WHEN vote_cast IN ('No', 'Nay') THEN 1
                 ELSE 0
                 END nay_or_no_vote
          , case 
                 WHEN vote_cast = 'Present' THEN 1
                 ELSE 0
                 END abstain_vote
       ,chamber
FROM `Congress_Stg.senate_votes_External`
,UNNEST(roll_call_vote.members.member) member_unnested),

votes_by_party as(
    select 
           -- bring in bill number and type to senate and house votes
           -- Add Majority
           congress,
           session,
           vote_number rollcall_num,
           bill_key,
       replace(chamber, 'House', 'U.S. House of Representatives') chamber,

       sum(case party when 'D' THEN yea_or_aye_vote ELSE 0 END) D_yea_or_aye_votes,
       sum(case party when 'R' THEN yea_or_aye_vote ELSE 0 END) R_yea_or_aye_votes,
       sum(case party when 'I' THEN yea_or_aye_vote ELSE 0 END) I_yea_or_aye_votes,
       
       sum(case party when 'D' THEN nay_or_no_vote ELSE 0 END) D_nay_or_no_votes,
       sum(case party when 'R' THEN nay_or_no_vote ELSE 0 END) R_nay_or_no_votes,
       sum(case party when 'I' THEN nay_or_no_vote ELSE 0 END) I_nay_or_no_votes,

       sum(case party when 'D' THEN abstain_vote ELSE 0 END) D_abstain_votes,
       sum(case party when 'R' THEN abstain_vote ELSE 0 END) R_abstain_votes,
       sum(case party when 'I' THEN abstain_vote ELSE 0 END) I_abstain_votes,

       current_datetime() as flowDate

    from sen_votes_categorized
    group by all
)

select distinct 
       -- Add bill key in DBT
       CAST(svc.vote_number AS INT) roll_call_number,
       svc.bill_key,
       FORMAT_DATE('%Y-%m-%d', PARSE_DATETIME('%B %d, %Y, %I:%M %p', svc.vote_date)) as vote_date,
       CAST(svc.congress AS INT) congress, 
       CAST(svc.session AS INT) session,
       svc.bill_type,
       svc.chamber,
       svc.bill_name,
       question,
       svc.vote_result result,
       COALESCE(CAST(svc.count.yeas AS INT), 0) yea_totals,
       COALESCE(CAST(svc.count.nays AS INT), 0) nay_totals,
       COALESCE(CAST(svc.count.present AS INT), 0) present_totals,
       COALESCE(CAST(svc.count.absent AS INT), 0) not_voting_totals,
       vp.* EXCEPT(congress, session, rollcall_num, bill_key, chamber), 
from votes_by_party vp join sen_votes_categorized svc on(vp.bill_key = svc.bill_key
                                                         AND vp.congress=svc.congress 
                                                         AND vp.session = svc.session
                                                         AND vp.rollcall_num =svc.vote_number)


-- ,

-- select *
-- from `Congress_Stg.senate_votes_External`
-- where bill_number = 7024 AND bill_type = 'HR'


-- select distinct member_unnested.vote_cast
-- FROM `Congress_Stg.senate_votes_External`
-- ,UNNEST(roll_call_vote.members.member) member_unnested