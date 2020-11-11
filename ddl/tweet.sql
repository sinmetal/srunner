CREATE TABLE Tweet (
	Id STRING(MAX) NOT NULL,
	Author STRING(MAX) NOT NULL,
	Count INT64 NOT NULL,
	Favos ARRAY<STRING(MAX)> NOT NULL,
	Sort INT64 NOT NULL,
	ShardCreatedAt INT64 NOT NULL,
	CreatedAt TIMESTAMP NOT NULL,
	UpdatedAt TIMESTAMP NOT NULL,
	CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	NewSimpleColumn INT64,
	SchemaVersion INT64,
	Content STRING(MAX),
) PRIMARY KEY (Id);

CREATE INDEX TweetAuthor
ON Tweet (
	Author
);

CREATE INDEX TweetAuthorCommitedAtDesc
ON Tweet (
	Author,
	CommitedAt DESC
);

CREATE INDEX TweetAuthorCreatedAtDesc
ON Tweet (
	Author,
	CreatedAt DESC
);

CREATE INDEX TweetAuthorSort
ON Tweet (
	Author,
	Sort
);

CREATE INDEX TweetSchemaVersion
ON Tweet (
	SchemaVersion
);

CREATE INDEX TweetShardCreatedAtAscCreatedAtDesc
ON Tweet (
	ShardCreatedAt,
	CreatedAt DESC
);

CREATE INDEX TweetShardCreatedAtAscUpdatedAtDesc
ON Tweet (
	ShardCreatedAt,
	UpdatedAt DESC
);

CREATE INDEX TweetSort
ON Tweet (
	Sort
);