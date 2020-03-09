CREATE TABLE ItemMaster (
	ItemID STRING(MAX) NOT NULL,
	Name STRING(MAX) NOT NULL,
	Price INT64 NOT NULL,
	CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ItemID);

CREATE INDEX ItemMasterPriceDesc
ON ItemMaster (
	Price DESC
);