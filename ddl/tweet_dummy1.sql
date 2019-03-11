CREATE TABLE TweetDummy1 (
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

CREATE INDEX TweetDummy1SortAsc
ON TweetDummy1 (
	Sort
);

CREATE INDEX TweetDummy1ShardCreatedAtAscCreatedAtDesc
ON TweetDummy1 (
	ShardCreatedAt,
	CreatedAt DESC
);

