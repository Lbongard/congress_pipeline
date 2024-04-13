# bill_status_ddl = """CREATE OR REPLACE EXTERNAL TABLE Congress.bill_status_external_table
# OPTIONS (
#   format = 'JSON',
#   uris = [
#     'gs://congress_data/bills/hconres/*.json',
#     'gs://congress_data/bills/hjres/*.json',
#     'gs://congress_data/bills/hr/*.json',
#     'gs://congress_data/bills/s/*.json',
#     'gs://congress_data/bills/sjres/*.json',
#     'gs://congress_data/bills/sres/*.json'
#   ]);

# """


bill_status_ddl = """CREATE OR REPLACE EXTERNAL TABLE Congress.bill_status_external_table(
   bill STRUCT<
        number STRING,
        updateDate TIMESTAMP,
        type STRING,
        introducedDate DATE,
        congress INT64,
        committees STRUCT<
            item ARRAY<STRUCT<
                name STRING,
                chamber STRING,
                type STRING
            >>
        >,
        actions STRUCT<
            item ARRAY<STRUCT<
                actionDate DATE,
                text STRING,
                type STRING,
                actionCode STRING,
                recordedVotes ARRAY<STRUCT<
                    rollNumber STRING,
                    url STRING,
                    chamber STRING,
                    congress INT64,
                    date TIMESTAMP,
                    sessionNumber STRING
                >>
            >>
        >,
        sponsors STRUCT<
            item ARRAY<STRUCT<
                bioguideID STRING,
                fulName STRING,
                firstName STRING,
                lastName STRING
            >>
        >,
        cosponsors STRUCT<
            count INT64,
            item ARRAY<STRUCT<
                bioguideID STRING
            >>
        >,
        policyArea STRUCT<
            name STRING
        >,
        subjects STRUCT<
            legislativeSubjects STRUCT<
                item ARRAY<STRUCT<
                    name STRING
                >>
            >
        >,
        title STRING,
        latestAction STRUCT<
            actionDate DATE,
            text STRING
        >
    >
)

OPTIONS (
  format = 'JSON',
  uris = [
    'gs://congress_data/bill_status/hconres/*.json',
    'gs://congress_data/bills_status/hjres/*.json',
    'gs://congress_data/bills_status/hr/*.json',
    'gs://congress_data/bills_status/s/*.json',
    'gs://congress_data/bills_status/sjres/*.json',
    'gs://congress_data/bills_status/sres/*.json'
  ]);

"""

vote_ddl = """CREATE OR REPLACE EXTERNAL TABLE Congress.votes_external_table (
  vote STRUCT<
    date DATE,
    bill_type STRING,
    bill_number STRING,
    roll_call_number STRING,
    result STRING,
    totals STRUCT<
      yea INTEGER,
      nay INTEGER,
      present INTEGER,
      not_voting INTEGER
    >
  >,
  legislator ARRAY<STRUCT<
    id STRING,
    first_name STRING,
    last_name STRING,
    party STRING,
    state STRING,
    vote STRING
  >>
)
OPTIONS (
  format = "JSON",
  uris = ['gs://congress_data/votes/HCONRES/*.json',
    'gs://congress_data/votes/HJRES/*.json',
    'gs://congress_data/votes/HR/*.json',
    'gs://congress_data/votes/S/*.json',
    'gs://congress_data/votes/SJRES/*.json',
    'gs://congress_data/votes/SRES/*.json']
)"""

member_ddl = """CREATE OR REPLACE EXTERNAL TABLE Congress.members_external_table (
  bioguideId STRING,
  state STRING,
  partyName STRING,
  name STRING,
  terms STRUCT<
  item ARRAY<STRUCT<chamber STRING, startYear INT64, endYear INT64>>
        >,
  updateDate STRING,
  url STRING,
  district FLOAT64,
  depiction STRUCT<attribution STRING, 
                   imageURL STRING>,
  depiction_key STRING,
  depiction_value STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://congress_data/members/*.json']
  )
"""



bill_status_schema = [
        {"name": "version", "type": "STRING"},
        {"name": "dublincore", "type": "RECORD", "fields": [
            {"name": "identifier", "type": "STRING"},
            {"name": "title", "type": "STRING"},
            {"name": "creator", "type": "STRING"},
            {"name": "subject", "type": "STRING", "mode": "REPEATED"},
            {"name": "description", "type": "STRING"},
            {"name": "publisher", "type": "STRING"},
            {"name": "contributor", "type": "STRING"},
            {"name": "date", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {"name": "format", "type": "STRING"},
            {"name": "source", "type": "STRING"},
            {"name": "language", "type": "STRING"},
            {"name": "relation", "type": "STRING"},
            {"name": "coverage", "type": "STRING"},
            {"name": "rights", "type": "STRING"},
            {"name": "xmlnsdc", "type": "STRING"},
            {"name": "dcformat", "type": "STRING"},
            {"name": "dclanguage", "type": "STRING"},
            {"name": "dcrights", "type": "STRING"},
            {"name": "dccontributor", "type": "STRING"},
            {"name": "dcdescription", "type": "STRING"}
        ]},
        {"name": "bill", "type": "RECORD", "fields": [
            {"name": "number", "type": "STRING"},
            {"name": "updateDate", "type": "STRING"},
            {"name": "updateDateIncludingText", "type": "STRING"},
            {"name": "originChamber", "type": "STRING"},
            {"name": "originChamberCode", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {"name": "introducedDate", "type": "STRING"},
            {"name": "congress", "type": "STRING"},
            {"name": "ConstitutionalAuthorityStatementText", "type": "STRING"},
            {"name": "committees", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "systemCode", "type": "STRING"},
                    {"name": "name", "type": "STRING"},
                    {"name": "chamber", "type": "STRING"},
                    {"name": "type", "type": "STRING"},
                    {"name": "activities", "type": "RECORD", "fields": [
                        {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                            {"name": "name", "type": "STRING"},
                            {"name": "date", "type": "STRING"}
                        ]}
                    ]}
                ]}
            ]},
            {"name": "committeeReports", "type": "RECORD", "fields": [
                {"name": "committeeReport", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "citation", "type": "STRING"}
                ]}
            ]},
            {"name": "actions", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "actionDate", "type": "STRING"},
                    {"name": "committees", "type": "RECORD", "fields": [
                        {"name": "item", "type": "RECORD", "fields": [
                            {"name": "systemCode", "type": "STRING"},
                            {"name": "name", "type": "STRING"}
                        ]}
                    ]},
                    {"name": "sourceSystem", "type": "RECORD", "fields": [
                        {"name": "name", "type": "STRING"},
                        {"name": "code", "type": "STRING"}
                    ]},
                    {"name": "text", "type": "STRING"},
                    {"name": "type", "type": "STRING"},
                    {"name": "actionTime", "type": "STRING"},
                    {"name": "actionCode", "type": "STRING"},
                    {"name": "recordedVotes", "type": "RECORD", "fields": [
                        {"name": "recordedVote", "type": "RECORD", "fields": [
                            {"name": "rollNumber", "type": "STRING"},
                            {"name": "url", "type": "STRING"},
                            {"name": "chamber", "type": "STRING"},
                            {"name": "congress", "type": "STRING"},
                            {"name": "date", "type": "STRING"},
                            {"name": "sessionNumber", "type": "STRING"}
                        ]}
                    ]},
                    {"name": "calendarNumber", "type": "RECORD", "fields": [
                        {"name": "calendar", "type": "STRING"}
                    ]}
                ]}
            ]},
            {"name": "sponsors", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "bioguideId", "type": "STRING"},
                    {"name": "fullName", "type": "STRING"},
                    {"name": "firstName", "type": "STRING"},
                    {"name": "lastName", "type": "STRING"},
                    {"name": "party", "type": "STRING"},
                    {"name": "state", "type": "STRING"},
                    {"name": "middleName", "type": "STRING"},
                    {"name": "district", "type": "STRING"},
                    {"name": "isByRequest", "type": "STRING"}
                ]}
            ]},
            {"name": "cosponsors", "type": "RECORD", "fields": [
                {"name": "countIncludingWithdrawnCosponsors", "type": "STRING"},
                {"name": "count", "type": "STRING"},
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "district", "type": "STRING"},
                    {"name": "bioguideId", "type": "STRING"},
                    {"name": "fullName", "type": "STRING"},
                    {"name": "firstName", "type": "STRING"},
                    {"name": "lastName", "type": "STRING"},
                    {"name": "party", "type": "STRING"},
                    {"name": "state", "type": "STRING"},
                    {"name": "sponsorshipDate", "type": "STRING"},
                    {"name": "isOriginalCosponsor", "type": "STRING"},
                    {"name": "middleName", "type": "STRING"}
                ]}
            ]},
            {"name": "cboCostEstimates", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "pubDate", "type": "STRING"},
                    {"name": "title", "type": "STRING"},
                    {"name": "url", "type": "STRING"},
                    {"name": "description", "type": "STRING"}
                ]}
            ]},
            {"name": "policyArea", "type": "RECORD", "fields": [
                {"name": "name", "type": "STRING"}
            ]},
            {"name": "subjects", "type": "RECORD", "fields": [
                {"name": "legislativeSubjects", "type": "RECORD", "fields": [
                    {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                        {"name": "name", "type": "STRING"}
                    ]}
                ]},
                {"name": "policyArea", "type": "RECORD", "fields": [
                    {"name": "name", "type": "STRING"}
                ]}
            ]},
            {"name": "summaries", "type": "RECORD", "fields": [
                {"name": "summary", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "versionCode", "type": "STRING"},
                    {"name": "actionDate", "type": "STRING"},
                    {"name": "actionDesc", "type": "STRING"},
                    {"name": "updateDate", "type": "STRING"},
                    {"name": "text", "type": "STRING"}
                ]}
            ]},
            {"name": "title", "type": "STRING"},
            {"name": "titles", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "titleType", "type": "STRING"},
                    {"name": "title", "type": "STRING"},
                    {"name": "billTextVersionName", "type": "STRING"},
                    {"name": "billTextVersionCode", "type": "STRING"},
                    {"name": "chamberCode", "type": "STRING"},
                    {"name": "chamberName", "type": "STRING"},
                    {"name": "sourceSystem", "type": "RECORD", "fields": [
                        {"name": "code", "type": "STRING"},
                        {"name": "name", "type": "STRING"}
                    ]}
                ]}
            ]},
            {"name": "textVersions", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "type", "type": "STRING"},
                    {"name": "date", "type": "STRING"},
                    {"name": "formats", "type": "RECORD", "fields": [
                        {"name": "item", "type": "RECORD", "fields": [
                            {"name": "url", "type": "STRING"}
                        ]}
                    ]}
                ]}
            ]},
            {"name": "latestAction", "type": "RECORD", "fields": [
                {"name": "actionDate", "type": "STRING"},
                {"name": "text", "type": "STRING"},
                {"name": "actionTime", "type": "STRING"}
            ]},
            {"name": "relatedBills", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "title", "type": "STRING"},
                    {"name": "congress", "type": "STRING"},
                    {"name": "number", "type": "STRING"},
                    {"name": "type", "type": "STRING"},
                    {"name": "latestAction", "type": "RECORD", "fields": [
                        {"name": "actionDate", "type": "STRING"},
                        {"name": "text", "type": "STRING"},
                        {"name": "actionTime", "type": "STRING"}
                    ]},
                    {"name": "relationshipDetails", "type": "RECORD", "fields": [
                        {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                            {"name": "type", "type": "STRING"},
                            {"name": "identifiedBy", "type": "STRING"}
                        ]}
                    ]}
                ]}
            ]},
            {"name": "laws", "type": "RECORD", "fields": [
                {"name": "item", "type": "RECORD", "fields": [
                    {"name": "type", "type": "STRING"},
                    {"name": "number", "type": "STRING"}
                ]}
            ]},
            {"name": "cdata", "type": "STRING"},
            {"name": "amendments", "type": "RECORD", "fields": [
                {"name": "amendment", "type": "RECORD", "mode": "REPEATED", "fields": [
                    {"name": "number", "type": "STRING"},
                    {"name": "congress", "type": "STRING"},
                    {"name": "type", "type": "STRING"},
                    {"name": "description", "type": "STRING"},
                    {"name": "purpose", "type": "STRING"},
                    {"name": "updateDate", "type": "STRING"},
                    {"name": "latestAction", "type": "RECORD", "fields": [
                        {"name": "actionDate", "type": "STRING"},
                        {"name": "text", "type": "STRING"},
                        {"name": "updateDate", "type": "STRING"},
                        {"name": "actionTime", "type": "STRING"},
                        {"name": "links", "type": "RECORD", "fields": [
                            {"name": "link", "type": "RECORD", "mode": "REPEATED", "fields": [
                                {"name": "name", "type": "STRING"},
                                {"name": "url", "type": "STRING"}
                            ]}
                        ]}
                    ]},
                    {"name": "sponsors", "type": "RECORD", "fields": [
                        {"name": "item", "type": "RECORD", "fields": [
                            {"name": "bioguideId", "type": "STRING"},
                            {"name": "fullName", "type": "STRING"},
                            {"name": "firstName", "type": "STRING"},
                            {"name": "lastName", "type": "STRING"},
                            {"name": "party", "type": "STRING"},
                            {"name": "state", "type": "STRING"},
                            {"name": "district", "type": "STRING"},
                            {"name": "name", "type": "STRING"},
                            {"name": "middleName", "type": "STRING"}
                        ]}
                    ]},
                    {"name": "submittedDate", "type": "STRING"},
                    {"name": "chamber", "type": "STRING"},
                    {"name": "amendedBill", "type": "RECORD", "fields": [
                        {"name": "congress", "type": "STRING"},
                        {"name": "type", "type": "STRING"},
                        {"name": "originChamber", "type": "STRING"},
                        {"name": "originChamberCode", "type": "STRING"},
                        {"name": "number", "type": "STRING"},
                        {"name": "title", "type": "STRING"},
                        {"name": "updateDateIncludingText", "type": "STRING"}
                    ]},
                    {"name": "actions", "type": "RECORD", "fields": [
                        {"name": "count", "type": "STRING"},
                        {"name": "actions", "type": "RECORD", "fields": [
                            {"name": "item", "type": "RECORD", "mode": "REPEATED", "fields": [
                                {"name": "actionDate", "type": "STRING"},
                                {"name": "actionTime", "type": "STRING"},
                                {"name": "text", "type": "STRING"},
                                {"name": "type", "type": "STRING"},
                                {"name": "actionCode", "type": "STRING"},
                                {"name": "sourceSystem", "type": "RECORD", "fields": [
                                    {"name": "code", "type": "STRING"},
                                    {"name": "name", "type": "STRING"}
                                ]},
                                {"name": "recordedVotes", "type": "RECORD", "fields": [
                                    {"name": "recordedVote", "type": "RECORD", "fields": [
                                        {"name": "rollNumber", "type": "STRING"},
                                        {"name": "chamber", "type": "STRING"},
                                        {"name": "congress", "type": "STRING"},
                                        {"name": "date", "type": "STRING"},
                                        {"name": "sessionNumber", "type": "STRING"},
                                        {"name": "url", "type": "STRING"}
                                    ]}
                                ]},
                                {"name": "committees", "type": "RECORD", "fields": [
                                    {"name": "item", "type": "RECORD", "fields": [
                                        {"name": "systemCode", "type": "STRING"},
                                        {"name": "name", "type": "STRING"}
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                ]}
            ]},
            {"name": "proposedDate", "type": "STRING"}
        ]
    }
    ]


bill_schema = [
    {"name": "congress", "type": "INT64"},
    {"name": "latestAction", "type": "RECORD",
        "fields": [
            {"name": "actionDate", "type": "STRING"},
            {"name": "text", "type": "STRING"}
        ]
    },
    {"name": "number", "type": "STRING"},
    {"name": "originChamber", "type": "STRING"},
    {"name": "originChamberCode", "type": "STRING"},
    {"name": "title", "type": "STRING"},
    {"name": "type", "type": "STRING"},
    {"name": "updateDate", "type": "STRING"},
    {"name": "updateDateIncludingText", "type": "STRING"},
    {"name": "url", "type": "STRING"}
]

member_schema = [
    {"name": "bioguideID", "type": "STRING"},
    {"name": "state", "type": "STRING"},
    {"name": "partyName", "type": "STRING"},
    {"name": "name", "type": "STRING"},
    {"name": "terms", "type": "RECORD",
        "fields":[
            {"name":"chamber", "type": "STRING"},
            {"name":"endYear", "type": "INT64"},
            {"name":"startYear", "type": "INT64"}
        ]
    },
    {"name": "updateDate", "type": "STRING"},
    {"name": "url", "type": "STRING"},
    {"name": "district", "type": "FLOAT64"},
    {"name": "depiction", "type": "RECORD",
        "fields": [
            {"name": "attribution", "type": "STRING"},
            {"name": "imageUrl", "type": "STRING"}
        ]
    }
]

house_vote_schema = [
    {"name": "vote_metadata", "type": "RECORD", "fields": [
        {"name": "majority", "type": "STRING"},
        {"name": "congress", "type": "STRING"},
        {"name": "session", "type": "STRING"},
        {"name": "chamber", "type": "STRING"},
        {"name": "rollcall_num", "type": "STRING"},
        {"name": "legis_num", "type": "STRING"},
        {"name": "vote_question", "type": "STRING"},
        {"name": "vote_type", "type": "STRING"},
        {"name": "vote_result", "type": "STRING"},
        {"name": "action_date", "type": "STRING"},
        {"name": "action_time", "type": "RECORD", "fields": [
            {"name": "time_etz", "type": "STRING"},
            {"name": "text", "type": "STRING"}
        ]},
        {"name": "vote_desc", "type": "STRING"},
        {"name": "vote_totals", "type": "RECORD", "fields": [
            {"name": "totals_by_party_header", "type": "RECORD", "fields": [
                {"name": "party_header", "type": "STRING"},
                {"name": "yea_header", "type": "STRING"},
                {"name": "nay_header", "type": "STRING"},
                {"name": "present_header", "type": "STRING"},
                {"name": "not_voting_header", "type": "STRING"}
            ]},
            {"name": "totals_by_party", "type": "RECORD", "mode": "REPEATED", "fields": [
                {"name": "party", "type": "STRING"},
                {"name": "yea_total", "type": "STRING"},
                {"name": "nay_total", "type": "STRING"},
                {"name": "present_total", "type": "STRING"},
                {"name": "not_voting_total", "type": "STRING"}
            ]},
            {"name": "totals_by_vote", "type": "RECORD", "fields": [
                {"name": "total_stub", "type": "STRING"},
                {"name": "yea_total", "type": "STRING"},
                {"name": "nay_total", "type": "STRING"},
                {"name": "present_total", "type": "STRING"},
                {"name": "not_voting_total", "type": "STRING"}
            ]}
        ]}
    ]},
    {"name": "vote_data", "type": "RECORD", "fields": [
        {"name": "recorded_vote", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "legislator", "type": "RECORD", "fields": [
                {"name": "name_id", "type": "STRING"},
                {"name": "sort_field", "type": "STRING"},
                {"name": "unaccented_name", "type": "STRING"},
                {"name": "party", "type": "STRING"},
                {"name": "state", "type": "STRING"},
                {"name": "role", "type": "STRING"},
                {"name": "text", "type": "STRING"}
            ]},
            {"name": "vote", "type": "STRING"}
        ]}
    ]}
]


senate_votes_schema = [
    {"name": "congress", "type": "STRING"},
    {"name": "session", "type": "STRING"},
    {"name": "congress_year", "type": "STRING"},
    {"name": "vote_number", "type": "STRING"},
    {"name": "vote_date", "type": "STRING"},
    {"name": "modify_date", "type": "STRING"},
    {"name": "vote_question_text", "type": "STRING"},
    {"name": "vote_document_text", "type": "STRING"},
    {"name": "vote_result_text", "type": "STRING"},
    {"name": "question", "type": "STRING"},
    {"name": "vote_title", "type": "STRING"},
    {"name": "majority_requirement", "type": "STRING"},
    {"name": "vote_result", "type": "STRING"},
    {"name": "document", "type": "RECORD", "fields": [
        {"name": "document_congress", "type": "STRING"},
        {"name": "document_type", "type": "STRING"},
        {"name": "document_number", "type": "STRING"},
        {"name": "document_name", "type": "STRING"},
        {"name": "document_title", "type": "STRING"},
        {"name": "document_short_title", "type": "STRING"}
    ]},
    {"name": "amendment", "type": "RECORD", "fields": [
        {"name": "amendment_number", "type": "STRING"},
        {"name": "amendment_to_amendment_number", "type": "STRING"},
        {"name": "amendment_to_amendment_to_amendment_number", "type": "STRING"},
        {"name": "amendment_to_document_number", "type": "STRING"},
        {"name": "amendment_to_document_short_title", "type": "STRING"},
        {"name": "amendment_purpose", "type": "STRING"}
    ]},
    {"name": "count", "type": "RECORD", "fields": [
        {"name": "yeas", "type": "STRING"},
        {"name": "nays", "type": "STRING"},
        {"name": "present", "type": "STRING"},
        {"name": "absent", "type": "STRING"}
    ]},
    {"name": "tie_breaker", "type": "RECORD", "fields": [
        {"name": "by_whom", "type": "STRING"},
        {"name": "tie_breaker_vote", "type": "STRING"}
    ]},
    {"name": "members", "type": "RECORD", "fields": [
        {"name": "member", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "member_full", "type": "STRING"},
            {"name": "last_name", "type": "STRING"},
            {"name": "first_name", "type": "STRING"},
            {"name": "party", "type": "STRING"},
            {"name": "state", "type": "STRING"},
            {"name": "vote_cast", "type": "STRING"},
            {"name": "lis_member_id", "type": "STRING"}
        ]}
    ]}
]