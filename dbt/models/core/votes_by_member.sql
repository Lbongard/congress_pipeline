{{
    config(
        materialized='table',
        partition_by={
            "field": "vote_date",
            "data_type": "date",
            "granularity": "day"
        }
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
                   roll_call.congress,
                   CASE 
                       WHEN GREATEST(D_yea_or_aye_votes, D_nay_or_no_votes, D_abstain_votes) = D_yea_or_aye_votes THEN 'yea/aye'
                       WHEN GREATEST(D_yea_or_aye_votes, D_nay_or_no_votes, D_abstain_votes) = D_nay_or_no_votes THEN 'nay/no'
                       WHEN GREATEST(D_yea_or_aye_votes, D_nay_or_no_votes, D_abstain_votes) = D_abstain_votes THEN 'abstain' 
                   END AS dem_majority_vote,
                   CASE 
                       WHEN GREATEST(R_yea_or_aye_votes, R_nay_or_no_votes, R_abstain_votes) = R_yea_or_aye_votes THEN 'yea/aye'
                       WHEN GREATEST(R_yea_or_aye_votes, R_nay_or_no_votes, R_abstain_votes) = R_nay_or_no_votes THEN 'nay/no'
                       WHEN GREATEST(R_yea_or_aye_votes, R_nay_or_no_votes, R_abstain_votes) = R_abstain_votes THEN 'abstain' 
                   END AS rep_majority_vote,
                   CASE 
                       WHEN GREATEST(I_yea_or_aye_votes, I_nay_or_no_votes, I_abstain_votes) = I_yea_or_aye_votes THEN 'yea/aye'
                       WHEN GREATEST(I_yea_or_aye_votes, I_nay_or_no_votes, I_abstain_votes) = I_nay_or_no_votes THEN 'nay/no'
                       WHEN GREATEST(I_yea_or_aye_votes, I_nay_or_no_votes, I_abstain_votes) = I_abstain_votes THEN 'abstain' 
                   END AS ind_majority_vote,
                   CASE 
                       WHEN vote IN ('Yea', 'Aye') THEN 'yea/aye'
                       WHEN vote IN ('No', 'Nay') THEN 'nay/no'
                       WHEN vote ='Present' THEN 'abstain'
                       WHEN vote = 'Not Voting' THEN 'not_voting'
                   END AS member_vote,
                   bills.policyArea,
                   bills.title,
                   bills.url,
                   votes.vote_date,
                   votes.result,
                   votes.question,
            FROM {{ref('fact_roll_call_vote')}} roll_call
            JOIN {{ref('dim_votes')}} votes ON (roll_call.congress = votes.congress
                                                AND roll_call.session = votes.session
                                                AND roll_call.chamber = votes.chamber
                                                AND roll_call.roll_call_number = votes.roll_call_number 
                                                AND roll_call.bill_name = votes.bill_name)
            LEFT JOIN {{ref('dim_members')}} mems_house ON (roll_call.bioguideID = mems_house.bioguideID)
            LEFT JOIN {{ref('dim_members')}} mems_sen ON (roll_call.lisid = mems_sen.lisid)
            LEFT JOIN {{ref('dim_bills')}} bills on (roll_call.bill_name = bills.bill_name
                                                     AND roll_call.congress = bills.congress)
        )
          SELECT distinct 
                name,
                chamber,
                congress,
                roll_call_number,
                bioguideID,
                partyName,
                member_vote,
                bill_name,
                policyArea,
                title,
                question,
                url,
                CAST(vote_date AS DATE) vote_date,
                result,
                dem_majority_vote,
                rep_majority_vote,
                CASE WHEN dem_majority_vote = member_vote THEN 1 ELSE 0 END voted_with_dem_majority,
                CASE WHEN rep_majority_vote = member_vote THEN 1 ELSE 0 END voted_with_rep_majority,
                CASE  
                    WHEN partyName = 'Democratic' AND dem_majority_vote = member_vote THEN 1
                    WHEN partyName = 'Republican' AND rep_majority_vote = member_vote THEN 1
                    WHEN partyName = 'Independent' AND ind_majority_vote = member_vote THEN 1
                    ELSE 0
                END AS voted_with_party_majority
          FROM joined_data