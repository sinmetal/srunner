CREATE TABLE ItemOrderDummyFK (
    ItemOrderID STRING(MAX) NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    ItemID STRING(MAX) NOT NULL,
    UserID STRING(MAX) NOT NULL,
) PRIMARY KEY (ItemOrderID);

CREATE NULL_FILTERED INDEX ItemOrderDummyFK_ItemID
ON ItemOrderDummyFK (
    ItemID
);

CREATE NULL_FILTERED INDEX ItemOrderDummyFK_UserID
ON ItemOrderDummyFK (
    UserID
);