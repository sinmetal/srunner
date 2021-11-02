CREATE TABLE ScoreUser (
  Id STRING(MAX) NOT NULL,
  CommitedAt TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY (Id);

CREATE TABLE Score (
  Id STRING(MAX) NOT NULL,
  ClassRank INT64 AS (IF(Score > 1000000000, 6, IF(Score > 100000000, 5, IF(Score > 10000000, 4, IF(Score > 1000000, 3, IF(Score > 100000, 2, IF(Score > 10000, 1, 0))))))) STORED,
  CircleID STRING(MAX) NOT NULL,
  Score INT64 NOT NULL,
  MaxScore INT64 NOT NULL,
  CommitedAt TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  Shard INT64,
) PRIMARY KEY (Id);

CREATE INDEX CommitedAtDescByScore
ON Score (
	CommitedAt DESC
);

CREATE INDEX ScoreByMaxScoreDesc
ON Score (
	MaxScore DESC
);

CREATE INDEX ScoreByScoreDesc
ON Score (
	Score DESC
);

CREATE INDEX ShardCommitedAtDescByScore
ON Score (
	Shard,
	CommitedAt DESC
);