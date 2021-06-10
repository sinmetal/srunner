CREATE TABLE ScoreUser (
  Id STRING(MAX) NOT NULL,
  CommitedAt TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY (Id);

CREATE TABLE Score (
  Id STRING(MAX) NOT NULL,
  ClassRank INT64 AS (IF(Score > 1000000000, 6, IF(Score > 100000000, 5, IF(Score > 10000000, 4, IF(Score > 1000000, 3, IF(Score > 100000, 2, IF(Score > 10000, 1, 0))))))) STORED,
  CircleID STRING(MAX),
  Score INT64,
  MaxScore INT64,
  CommitedAt TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY (Id);
