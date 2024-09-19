CREATE TABLE UserAccount (
    UserID STRING(MAX) NOT NULL,
    Age INT64,
    Height INT64,
    Weight INT64,
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID);

CREATE INDEX AgeAndHeightByUserAccount
ON UserAccount (
    Age,
    Height
);

CREATE INDEX AgeAndWeightByUserAccount
ON UserAccount (
    Age,
    Weight
);

CREATE TABLE UserBalance (
	UserID STRING(MAX) NOT NULL,
	Amount INT64 NOT NULL,
	Point INT64 NOT NULL,
	CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID);

CREATE TABLE UserDepositHistory (
    UserID STRING(MAX) NOT NULL,
    DepositID STRING(MAX) NOT NULL,
    DepositType int64 NOT NULL,
	Amount INT64 NOT NULL,
	Point INT64 NOT NULL,
    SumVersion STRING(MAX),
    SupplementaryInformation JSON,
	CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID, DepositID);

CREATE INDEX DepositTypeByUserDepositHistory
ON UserDepositHistory (
    DepositType
);

CREATE INDEX DepositTypeStoredAmountAndPointByUserDepositHistory
ON UserDepositHistory (
    DepositType
) STORING (
	Amount,
	Point
);

CREATE TABLE UserDepositHistorySum (
    UserID STRING(MAX) NOT NULL,
    Amount INT64 NOT NULL,
    Point INT64 NOT NULL,
    Count INT64 NOT NULL,
    Note STRING(MAX),
    UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID);
