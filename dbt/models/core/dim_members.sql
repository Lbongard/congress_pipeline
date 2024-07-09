
-- -- with terms as(
-- -- select
-- --        chamber,
-- --        term_end_year - term_start_year tenure_by_term
-- -- from {{ref("stg_members")}}
-- -- ),

-- -- tenure_calculated as (
-- --     select chamber, sum(tenure_by_term)
-- --     from terms
-- --     group by chamber
-- -- ),

-- -- current_term_info as(

-- -- select distinct bioguideID
-- --                 state,
-- --                 partyName,
-- --                 district,
-- --                 imageURL
-- -- from {{ref("stg_members")}})

-- -- select * 
-- -- from current_term_info

-- with members_staged as(

-- select distinct * except(depiction)
-- from {{ref("stg_members")}}),


-- terms_numbered as(
--     select *, row_number() over(partition by bioguideID, chamber order by term_start_year desc) row_num
--     from {{ref("stg_terms")}}
--     where term_end_year IS NULL
--     group by all
-- ),

-- current_chamber as(
--   select *
--   from terms_numbered
--   where row_num = 1
-- )

-- SELECT mems.*, chmbr.chamber chamber
-- FROM members_staged mems left join current_chamber chmbr 
--     on mems.bioguideID = chmbr.bioguideID


{{
    config(
        materialized='table'
    )
}}

with members_staged as(

  select distinct * except(depiction)
  from {{ref("stg_members")}}),

senate_ids as(
  -- overrides used to fill in incorrect / missing data in list from senate website
  select distinct ids.first_name,
                  ids.last_name,
                  ids.state,
                  ids.bioguideID,
                  COALESCE(overrides.lisid, ids.lisid) lisid
  from {{ref("stg_senate_ids")}} ids left join {{ref("senate_id_overrides")}} overrides on ids.bioguideID = overrides.bioguideID
),


still_serving as(
    select * 
           ,row_number() over(partition by bioguideID, chamber order by term_start_year desc) row_num
           ,1 as currently_serving
    from {{ref("stg_terms")}}
    where term_end_year IS NULL
),

no_longer_serving as(
  select *
         ,row_number() over(partition by bioguideID order by term_end_year desc) row_num
         ,0 as currently_serving
  from {{ref("stg_terms")}}
  where bioguideID not in (select bioguideID from still_serving)
),

current_chamber as(
  select *
  from still_serving
  where row_num = 1
  
  UNION DISTINCT 

  select *
  from no_longer_serving
  where row_num = 1
)

SELECT mems.* 
       ,chmbr.chamber most_recent_chamber
       ,chmbr.currently_serving currently_serving
       , senate_ids.lisid
FROM members_staged mems 
      left join current_chamber chmbr on (mems.bioguideID = chmbr.bioguideID)
      left join senate_ids on (mems.bioguideID = senate_ids.bioguideID)
