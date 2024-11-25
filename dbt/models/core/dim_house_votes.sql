{{
    config(
        materialized='view'
    )
}}

WITH source AS(
  SELECT *
  FROM {{ref("stg_house_votes")}}),

vote_metadata_unnested AS(
  SELECT vote_metadata.*,
         bill_number,
         bill_type
  FROM source
),


vote_totals_unnested as(
  SELECT distinct
         CAST(rollcall_num AS INT) as roll_call_number,
         {{ dbt_utils.generate_surrogate_key(['bill_type', 'bill_number'])}} as bill_key,
         FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%d-%b-%Y',action_date)) vote_date,
         CAST(congress AS INT) congress,
         CAST(left(session, 1) AS INT) session,
         bill_type,
         REPLACE(chamber, 'U.S. ','') chamber,
         concat(bill_type,bill_number) bill_name,
         vote_question as question,
         vote_result result,
         CAST(vote_totals.totals_by_vote.yea_total AS INT) yea_totals,
         CAST(vote_totals.totals_by_vote.nay_total AS INT) nay_totals,
         CAST(vote_totals.totals_by_vote.present_total AS INT) present_totals,
         CAST(vote_totals.totals_by_vote.not_voting_total AS INT) not_voting_totals,
         vote_totals_by_party_unnested.*
  FROM vote_metadata_unnested
       ,UNNEST(vote_totals.totals_by_party) vote_totals_by_party_unnested
)
,

vote_totals_by_party as(
SELECT congress,
       bill_key,
       session,
       CAST(roll_call_number AS INT) roll_call_number,
       sum(case party when 'Democratic' THEN CAST(yea_total AS INT) ELSE 0 END) D_yea_or_aye_votes,
       sum(case party when 'Republican' THEN CAST(yea_total AS INT) ELSE 0 END) R_yea_or_aye_votes,
       sum(case party when 'Independent' THEN CAST(yea_total AS INT) ELSE 0 END) I_yea_or_aye_votes,

       sum(case party when 'Democratic' THEN CAST(nay_total AS INT) ELSE 0 END) D_nay_or_no_votes,
       sum(case party when 'Republican' THEN CAST(nay_total AS INT) ELSE 0 END) R_nay_or_no_votes,
       sum(case party when 'Independent' THEN CAST(nay_total AS INT) ELSE 0 END) I_nay_or_no_votes,

       sum(case party when 'Democratic' THEN CAST(present_total AS INT) ELSE 0 END) D_abstain_votes,
       sum(case party when 'Republican' THEN CAST(present_total AS INT) ELSE 0 END) R_abstain_votes,
       sum(case party when 'Independent' THEN CAST(present_total AS INT) ELSE 0 END) I_abstain_votes
FROM vote_totals_unnested
GROUP BY ALL
)

SELECT distinct 
       tvu.* EXCEPT(party, yea_total, nay_total, present_total, not_voting_total),
       party_totals.* EXCEPT(congress, session, roll_call_number, bill_key)
FROM vote_totals_unnested tvu join vote_totals_by_party party_totals ON (tvu.congress = party_totals.congress
                                                                         AND tvu.session = party_totals.session
                                                                         AND tvu.roll_call_number = party_totals.roll_call_number
                                                                         AND tvu.bill_key = party_totals.bill_key)