ATTACH TABLE _ UUID '8a7ce161-cc28-44fc-9f3e-71d5aaa64a33'
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
ENGINE = ReplacingMergeTree
PRIMARY KEY (GLOBALEVENTID, MentionIdentifier)
ORDER BY (GLOBALEVENTID, MentionIdentifier, MentionTimeDate)
SETTINGS index_granularity = 8192
