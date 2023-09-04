CREATE TABLE Items (
    ItemID STRING(64) NOT NULL,
    ItemName STRING(1024) NOT NULL,
    Price INT64 NOT NULL,
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ItemID);

CREATE TABLE Orders (
    OrderID STRING(64) NOT NULL,
    UserID STRING(64) NOT NULL,
    Amount INT64 NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (OrderID);

# UserIDAndCommitedAtDescByOrders があるので、これは要らない
CREATE INDEX UserIDByOrders
ON Orders (
    UserID
);

CREATE INDEX UserIDAndCommitedAtDescByOrders
ON Orders (
    UserID,
    CommitedAt DESC
);

CREATE TABLE OrderDetails (
    OrderID STRING(64) NOT NULL,
    OrderDetailID STRING(64) NOT NULL,
    ItemID STRING(64) NOT NULL,
    Price INT64 NOT NULL,
    Quantity INT64 NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (OrderID,OrderDetailID);

CREATE INDEX ItemIDByOrderDetails
ON OrderDetails (
    ItemID
);
