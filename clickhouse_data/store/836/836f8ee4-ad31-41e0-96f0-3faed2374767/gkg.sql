ATTACH TABLE _ UUID 'b621b441-8a89-4fc3-b22e-cb88fd36e325'
(
    `GKGRECORDID` String,
    `DATE` DateTime,
    `SourceCollectionIdentifier` Int32,
    `SourceCommonName` String,
    `DocumentIdentifier` String,
    `Counts` String,
    `V2Themes` String,
    `Persons` String,
    `Organizations` String,
    `V2Tone` String,
    `GCAM` String
)
ENGINE = ReplacingMergeTree
PRIMARY KEY GKGRECORDID
ORDER BY (GKGRECORDID, DATE)
SETTINGS index_granularity = 8192
