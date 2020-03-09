CREATE TABLE User (
	UserID STRING(MAX) NOT NULL,
	Name STRING(MAX) NOT NULL,
	CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID)