with 
source as(
    select * from {{ref("stg_bills")}}
)

SELECT bill_key,
       JSON_EXTRACT_SCALAR(leg_subjects, '$.name') subject
FROM Congress_Stg.stg_bills,
UNNEST(JSON_EXTRACT_ARRAY(subjects, '$.legislativeSubjects.item')) leg_subjects