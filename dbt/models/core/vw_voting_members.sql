{{
    config(
        materialized='view'
        ,partition_by={
            "field": "introducedDate",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with members_by_congress as(
    select distinct  invertedOrderName 
                    ,lastName
                    ,terms.stateCode
                    ,terms.stateName
                    ,dists.congressional_district
                    ,mems.bioguideID
                    ,imageURL
                    ,mostRecentParty
                    ,chamber
                    ,terms.congress
    from {{ref('dim_members')}} mems 
            JOIN {{ref('dim_terms')}} terms on (mems.bioguideID = terms.bioguideID)
            LEFT JOIN {{ref('dim_congressional_districts')}} dists on (terms.district = dists.congressional_district)
        WHERE (mems.bioguideID in (SELECT distinct bioguideID from {{ref('fact_roll_call_vote')}}) OR
                mems.lisid in (SELECT distinct lisid from {{ref('fact_roll_call_vote')}}))
                )

select *, concat(invertedOrderName, " | ", LEFT(mostRecentParty, 1), " - ", stateCode) concat_name
from members_by_congress