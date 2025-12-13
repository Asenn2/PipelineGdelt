ATTACH TABLE _ UUID 'de5d815c-ce3c-4068-bbce-67e5ae46053f'
(
    `GKGRECORDID` String,
    `DATE` DateTime,
    `SourceCollectionIdentifier` Int32,
    `SourceCommonName` String,
    `DocumentIdentifier` String,
    `Counts` String,
    `V2Themes` String,
    `V2Tone` String,
    `GCAM` String
)
ENGINE = MergeTree
ORDER BY (DATE, GKGRECORDID)
SETTINGS index_granularity = 8192
