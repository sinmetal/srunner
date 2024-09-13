CREATE TABLE LockTrys (
    LockTryID STRING(MAX) NOT NULL,
    UserID STRING(MAX) NOT NULL,
    Number INT64,
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (LockTryID);

CREATE INDEX UserIDByLockTrys
ON LockTrys (
    UserID
);