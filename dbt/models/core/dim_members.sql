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

with members_staged as(

select distinct * except(depiction)
from {{ref("stg_members")}}),


terms_numbered as(
    select *, row_number() over(partition by bioguideID, chamber order by term_start_year desc) row_num
    from {{ref("stg_terms")}}
    where term_end_year IS NULL
    group by all
),

current_chamber as(
  select *
  from terms_numbered
  where row_num = 1
)

SELECT mems.*, chmbr.chamber chamber
FROM members_staged mems left join current_chamber chmbr 
    on mems.bioguideID = chmbr.bioguideID