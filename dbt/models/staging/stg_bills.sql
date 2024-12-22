with 

source as(
    select * from {{source('staging', 'bills_External')}}
)

-- ,flattened AS (
--   SELECT 
--     s.number AS bill_number,
--     s.updateDate as billUpdateDate,
--     -- updateDateIncludingText,
--     s.originChamber as bill_originChamber,
--     -- originChamberCode,
--     s.type AS bill_type,
--     s.introducedDate as billintroducedDate,
--     s.congress AS congress_number,
--     -- cdata,
--     committee_item.name AS committee_name,
--     committee_item.chamber AS commitee_chamber,
--     committee_item.type as committee_type,

--     sponsor_item.bioguideId AS sponsor_bioguideId,
--     sponsor_item.fullName AS sponsor_fullName,
--     -- sponsor_item.firstName AS sponsor_firstName,
--     -- sponsor_item.lastName AS sponsor_lastName,
--     -- sponsor_item.party AS sponsor_party,
--     -- sponsor_item.state AS sponsor_state,
--     sponsor_item.district AS sponsor_district,
--     -- sponsor_item.isByRequest AS sponsor_isByRequest,
--     cosponsor_item.bioguideId AS cosponsor_bioguideId,
--     -- cosponsor_item.fullName AS cosponsor_fullName,
--     -- cosponsor_item.firstName AS cosponsor_firstName,
--     -- cosponsor_item.lastName AS cosponsor_lastName,
--     -- cosponsor_item.party AS cosponsor_party,
--     -- cosponsor_item.state AS cosponsor_state,
--     -- cosponsor_item.middleName AS cosponsor_middleName,
--     -- cosponsor_item.district AS cosponsor_district,
--     -- cosponsor_item.sponsorshipDate AS cosponsor_sponsorshipDate,
--     -- cosponsor_item.isOriginalCosponsor AS cosponsor_isOriginalCosponsor,
--     title,
--     -- latestAction.actionDate AS latestActionDate,
--     -- latestAction.text AS latestActionText,
--     action_item.actionDate AS actionDate,
--     action_item.text AS actionText,
--     action_item.type AS actionType,
--     -- action_item.actionCode AS actionCode,
--     -- action_item.sourceSystem.code AS sourceSystemCode,
--     -- action_item.sourceSystem.name AS sourceSystemName,
--     -- action_committee_item.systemCode AS actionCommitteeSystemCode,
--     -- action_committee_item.name AS actionCommitteeName,
--     -- activity_item.name AS activityName,
--     -- activity_item.date AS activityDate
--   FROM source as s
--   ,UNNEST(s.committees.item) AS committee_item
--   ,UNNEST(sponsors.item) AS sponsor_item
--   ,UNNEST(cosponsors.item) AS cosponsor_item
--   ,UNNEST(actions.item) AS action_item
-- --   ,UNNEST(action_item.committees.item) AS action_committee_item
-- --   ,UNNEST(committee_item.activities.item) AS activity_item
-- )

SELECT  *
      , {{ dbt_utils.generate_surrogate_key(['type', 'number', 'congress'])}} as bill_key,
FROM source