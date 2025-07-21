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

with lagged_term_ends as(
SELECT bioguideID, 
       chamber, 
       term_start_year, 
       term_end_year, 
       lag(term_end_year) over(partition by bioguideID order by term_end_year asc) last_term_end,
       lag(chamber) over(partition by bioguideID order by term_end_year asc) last_chamber
from {{ref('dim_terms')}}
),

repeat_test as(
select *, 
      case when term_start_year = last_term_end and chamber = last_chamber THEN 0 else 1 end new_term
from lagged_term_ends),

grouped_terms as(
select *, sum(new_term) over(partition by bioguideID, chamber order by bioguideID, chamber, term_end_year asc) term_group
from repeat_test)

select bioguideID,
      chamber, 
      min(term_start_year) term_start_year,
      case when max(ifnull(term_end_year, 9999)) = 9999 THEN NULL else max(term_end_year) end term_end_year
from grouped_terms
group by all
order by bioguideID, term_start_year asc