CREATE TABLE Tweet (
    Id STRING(MAX) NOT NULL,
    Author STRING(MAX) NOT NULL,
    Content STRING(MAX) NOT NULL,
    Count INT64 NOT NULL,
    Favos ARRAY<STRING(MAX)> NOT NULL,
    Sort INT64 NOT NULL,
    ShardCreatedAt INT64 NOT NULL,
    CreatedAt TIMESTAMP NOT NULL,
    UpdatedAt TIMESTAMP NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Id);

CREATE INDEX Tweet
ON Tweet (
	Sort
);

CREATE INDEX TweetShardCreatedAtAscCreatedAtDesc
ON Tweet (
	ShardCreatedAt,
	CreatedAt DESC
);

