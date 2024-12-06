{{
    config(
        materialized='view',
        partition_by={
            "field": "vote_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


WITH vote_counts as(
    SELECT bill_key,
       roll_call_number,
       congress,
       party,
       vote,
       count(*) vote_count
    FROM {{ref('fact_roll_call_vote')}}
    group by all),

max_vote_counts as(
    SELECT *
    FROM vote_counts
    QUALIFY row_number() over(partition by bill_key, roll_call_number, congress, party order by vote_count desc) = 1
),

joined_data AS (
            SELECT roll_call.bill_name,
                   COALESCE(mems_house.bioguideID, mems_sen.bioguideID) bioguideID,
                   COALESCE(mems_house.invertedOrderName, mems_sen.invertedOrderName) name,
                   COALESCE(mems_house.mostRecentParty, mems_sen.mostRecentParty) partyName,
                   roll_call.chamber,
                   roll_call.vote,
                   roll_call.roll_call_number,
                   roll_call.congress,
                   mvc_d.vote AS dem_majority_vote,
                   mvc_r.vote AS rep_majority_vote,
                   mvc_i.vote AS ind_majority_vote,
                   roll_call.vote AS member_vote,
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
            LEFT JOIN max_vote_counts mvc_d on (mvc_d.bill_key = roll_call.bill_key 
                                                    AND mvc_d.roll_call_number = roll_call.roll_call_number
                                                    AND mvc_d.congress = roll_call.congress
                                                    AND mvc_d.party = 'D')
            LEFT JOIN max_vote_counts mvc_r on (mvc_r.bill_key = roll_call.bill_key 
                                                    AND mvc_r.roll_call_number = roll_call.roll_call_number
                                                    AND mvc_r.congress = roll_call.congress
                                                    AND mvc_r.party = 'R')
            LEFT JOIN max_vote_counts mvc_i on (mvc_i.bill_key = roll_call.bill_key 
                                                    AND mvc_i.roll_call_number = roll_call.roll_call_number
                                                    AND mvc_i.congress = roll_call.congress
                                                    AND mvc_i.party = 'I')
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