{{
    config(
        materialized='table'
    )
}}
-- with terms as(
-- select
--        chamber,
--        term_end_year - term_start_year tenure_by_term
-- from {{ref("stg_members")}}
-- ),

-- tenure_calculated as (
--     select chamber, sum(tenure_by_term)
--     from terms
--     group by chamber
-- ),

-- current_term_info as(

-- select distinct bioguideID
--                 state,
--                 partyName,
--                 district,
--                 imageURL
-- from {{ref("stg_members")}})

-- select * 
-- from current_term_info
                
select *
from {{ref("stg_members")}}