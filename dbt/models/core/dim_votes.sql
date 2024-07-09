{{
    config(
        materialized='table'
    )
}}

with votes_categorized as(
  select *, case 
                 WHEN vote IN ('Yea', 'Aye') THEN 1 
                 ELSE 0
                 END yea_or_aye_vote
          , case  
                 WHEN vote IN ('No', 'Nay') THEN 1
                 ELSE 0
                 END nay_or_no_vote
          , case 
                 WHEN vote = 'Present' THEN 1
                 ELSE 0
                 END abstain_vote
        --   , case
        --          WHEN vote = 'Not Voting' THEN 1
        --          ELSE 0
        --          END not_voting
FROM (select distinct * from {{ref("stg_votes")}})
),

votes_by_party as(
    select roll_call_number, 
       replace(chamber, 'House', 'House of Representatives') chamber,
       concat(bill_type, bill_number) bill_name,

       sum(case party when 'D' THEN yea_or_aye_vote ELSE 0 END) D_yea_or_aye_votes,
       sum(case party when 'R' THEN yea_or_aye_vote ELSE 0 END) R_yea_or_aye_votes,
       sum(case party when 'I' THEN yea_or_aye_vote ELSE 0 END) I_yea_or_aye_votes,
       
       sum(case party when 'D' THEN nay_or_no_vote ELSE 0 END) D_nay_or_no_votes,
       sum(case party when 'R' THEN nay_or_no_vote ELSE 0 END) R_nay_or_no_votes,
       sum(case party when 'I' THEN nay_or_no_vote ELSE 0 END) I_nay_or_no_votes,

       sum(case party when 'D' THEN abstain_vote ELSE 0 END) D_abstain_votes,
       sum(case party when 'R' THEN abstain_vote ELSE 0 END) R_abstain_votes,
       sum(case party when 'I' THEN abstain_vote ELSE 0 END) I_abstain_votes,

    from votes_categorized
    group by all
),

votes_info as(
select distinct {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
       roll_call_number,
       date vote_date,
       bill_type,
       replace(chamber, 'House', 'House of Representatives') chamber,
       concat(bill_type, bill_number) bill_name,
       result,
       cast(yea_totals as int) yea_totals,
       cast(nay_totals as int) nay_totals,
       yea_totals - nay_totals yea_nay_gap_votes,
       cast(present_totals as int) present_totals,
       cast(not_voting_totals as int) not_voting_totals

from {{ref("stg_votes")}})

select vi.*, 
       vbp.D_yea_or_aye_votes,
       vbp.R_yea_or_aye_votes,
       vbp.I_yea_or_aye_votes,
       vbp.D_nay_or_no_votes,
       vbp.R_nay_or_no_votes,
       vbp.I_nay_or_no_votes,
       vbp.D_abstain_votes,
       vbp.R_abstain_votes,
       vbp.I_abstain_votes
FROM votes_info vi left join votes_by_party vbp  
     on (vi.roll_call_number = vbp.roll_call_number 
         and vi.chamber = vbp.chamber 
         and vi.bill_name = vbp.bill_name)