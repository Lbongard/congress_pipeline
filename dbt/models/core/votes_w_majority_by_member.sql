{{
    config(
        materialized='table'
    )
}}


WITH joined_data AS (
            SELECT roll_call.bill_name,
                   COALESCE(mems_house.bioguideID, mems_sen.bioguideID) bioguideID,
                   COALESCE(mems_house.invertedOrderName, mems_sen.invertedOrderName) name,
                   COALESCE(mems_house.mostRecentParty, mems_sen.mostRecentParty) partyName,
                   roll_call.chamber,
                   roll_call.vote,
                   roll_call.roll_call_number,
                   CASE 
                       WHEN GREATEST(D_yea_or_aye_votes, D_nay_or_no_votes, D_present_votes) = D_yea_or_aye_votes THEN 'yea/aye'
                       WHEN GREATEST(D_yea_or_aye_votes, D_nay_or_no_votes, D_present_votes) = D_nay_or_no_votes THEN 'nay/no'
                       WHEN GREATEST(D_yea_or_aye_votes, D_nay_or_no_votes, D_present_votes) = D_present_votes THEN 'present' 
                   END AS dem_majority_vote,
                   CASE 
                       WHEN GREATEST(R_yea_or_aye_votes, R_nay_or_no_votes, R_present_votes) = R_yea_or_aye_votes THEN 'yea/aye'
                       WHEN GREATEST(R_yea_or_aye_votes, R_nay_or_no_votes, R_present_votes) = R_nay_or_no_votes THEN 'nay/no'
                       WHEN GREATEST(R_yea_or_aye_votes, R_nay_or_no_votes, R_present_votes) = R_present_votes THEN 'present' 
                   END AS rep_majority_vote,
                   CASE 
                       WHEN GREATEST(I_yea_or_aye_votes, I_nay_or_no_votes, I_present_votes) = I_yea_or_aye_votes THEN 'yea/aye'
                       WHEN GREATEST(I_yea_or_aye_votes, I_nay_or_no_votes, I_present_votes) = I_nay_or_no_votes THEN 'nay/no'
                       WHEN GREATEST(I_yea_or_aye_votes, I_nay_or_no_votes, I_present_votes) = I_present_votes THEN 'present' 
                   END AS ind_majority_vote,
                   CASE 
                       WHEN vote IN ('Yea', 'Aye') THEN 'yea/aye'
                       WHEN vote IN ('No', 'Nay') THEN 'nay/no'
                       WHEN vote ='Present' THEN 'present'
                       WHEN vote = 'Not Voting' THEN 'not_voting'
                   END AS member_vote
            FROM {{ref("fact_roll_call_vote")}} roll_call
            JOIN {{ref("dim_votes")}} votes ON (roll_call.roll_call_number = votes.roll_call_number AND roll_call.bill_name = votes.bill_name)
            LEFT JOIN {{ref("dim_members")}} mems_house ON (roll_call.bioguideID = mems_house.bioguideID)
            LEFT JOIN {{ref("dim_members")}} mems_sen ON (roll_call.lisid = mems_sen.lisid)
        ),
        votes_by_mem as(
          SELECT name,
                chamber,
                roll_call_number,
                bioguideID,
                partyName,
                member_vote,
                bill_name,
                CASE 
                    WHEN partyName = 'Democratic' AND dem_majority_vote = member_vote THEN 1
                    WHEN partyName = 'Republican' AND rep_majority_vote = member_vote THEN 1
                    WHEN partyName = 'Independent' AND ind_majority_vote = member_vote THEN 1
                    ELSE 0
                END AS voted_with_party_majority
          FROM joined_data)

        select name,
               bioguideID, 
               partyName,
               chamber,
               ROUND(
    SUM(CAST(voted_with_party_majority AS INT64)) / 
    SUM(CASE member_vote
                    WHEN 'not_voting' THEN 0 
                    ELSE 1 
                END),2) AS perc_votes_with_party_majority,
        from votes_by_mem
        WHERE bioguideID IS NOT NULL
        group by all