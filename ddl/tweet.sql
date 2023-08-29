CREATE TABLE Tweets (
    TweetID STRING(MAX) NOT NULL,
    Author STRING(MAX) NOT NULL,
    Content STRING(MAX),
    ContentLength INT64 NOT NULL AS (LENGTH(Content)) STORED,
    Favos ARRAY<STRING(MAX)> NOT NULL,
    Sort INT64 NOT NULL,
    SchemaVersion INT64,
    ShardID INT64 NOT NULL,
    CreatedAt TIMESTAMP NOT NULL,
    UpdatedAt TIMESTAMP NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (TweetID);
