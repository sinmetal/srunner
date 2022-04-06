CREATE TABLE UserBalance (
	UserID STRING(MAX) NOT NULL,
	Amount INT64 NOT NULL,
	Point INT64 NOT NULL,
	CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID);

CREATE TABLE UserDepositHistory (
	DepositID STRING(MAX) NOT NULL,
	UserID STRING(MAX) NOT NULL,
	Amount INT64 NOT NULL,
	Point INT64 NOT NULL,
	CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DepositID);

CREATE INDEX UserIDByUserDepositHistory
ON UserDepositHistory (
	UserID
);

CREATE INDEX UserIDStoredAmountAndPointByUserDepositHistory
ON UserDepositHistory (
	UserID
) STORING (
	Amount,
	Point
);