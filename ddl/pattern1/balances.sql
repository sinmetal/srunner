CREATE TABLE UserBalances (
	UserID STRING(64) NOT NULL,
	Amount INT64 NOT NULL,
	Point INT64 NOT NULL,
	CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID);

CREATE TABLE UserDepositHistories (
    DepositID STRING(64) NOT NULL,
    UserID STRING(64) NOT NULL,
	Amount INT64 NOT NULL,
	Point INT64 NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DepositID);

CREATE INDEX UserIDByUserDepositHistories
ON UserDepositHistories (
	UserID
);

CREATE INDEX UserIDStoredAmountAndPointByUserDepositHistories
ON UserDepositHistories (
	UserID
) STORING (
	Amount,
	Point
);