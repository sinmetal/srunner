CREATE TABLE Operation (
	Id STRING(MAX) NOT NULL,
	VERB STRING(MAX) NOT NULL,
	TargetKey STRING(MAX) NOT NULL,
	TargetTable STRING(MAX) NOT NULL,
	Body BYTES(MAX),
	CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Id);

CREATE INDEX OperationTargetKey
ON Operation (
TargetKey
);

CREATE INDEX OperationTargetTable
ON Operation (
TargetTable
);