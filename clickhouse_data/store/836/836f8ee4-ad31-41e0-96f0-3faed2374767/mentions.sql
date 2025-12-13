ATTACH TABLE _ UUID 'd8f59fdf-222c-4ffc-bdd6-0d2059e439f6'
(
    `GLOBALEVENTID` UInt64,
    `EventTimeDate` DateTime,
    `MentionTimeDate` DateTime,
    `MentionType` Int32,
    `MentionSourceName` String,
    `MentionIdentifier` String,
    `InRawText` Int32,
    `Confidence` Int32,
    `MentionDocLen` Int32,
    `MentionDocTone` Float32
)
ENGINE = MergeTree
ORDER BY (GLOBALEVENTID, MentionTimeDate)
SETTINGS index_granularity = 8192
