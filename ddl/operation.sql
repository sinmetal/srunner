CREATE TABLE Operation (
	OperationID STRING(MAX) NOT NULL,
    OperationName STRING(MAX) NOT NULL,
    ElapsedTimeMS INT64 NOT NULL,
    Note JSON,
	CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (OperationID);

CREATE INDEX OperationNameAndElapsedTimeMSByOperation
ON Operation (
  OperationName, ElapsedTimeMS DESC
);
