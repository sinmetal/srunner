CREATE TABLE UserAccount (
    UserId text NOT NULL,
    Age bigint NOT NULL,
    Height bigint NOT NULL,
    Weight bigint NOT NULL,
    CreatedAt timestamp with time zone NOT NULL DEFAULT NOW(),
    UpdatedAt timestamp with time zone NOT NULL DEFAULT NOW(),
    PRIMARY KEY (UserId)
);

CREATE TABLE UserBalance (
    UserId text NOT NULL,
    Amount bigint NOT NULL,
    Point bigint NOT NULL,
    CreatedAt timestamp with time zone NOT NULL DEFAULT NOW(),
    UpdatedAt timestamp with time zone NOT NULL DEFAULT NOW(),
    PRIMARY KEY (UserId)
);

CREATE TABLE UserDepositHistory (
    UserId text NOT NULL,
    DepositId text NOT NULL,
    DepositType bigint NOT NULL,
    Amount bigint NOT NULL,
    Point bigint NOT NULL,
    SumVersion text,
    SupplementaryInformation jsonb,
    CreatedAt timestamp with time zone NOT NULL DEFAULT NOW(),
    PRIMARY KEY (UserId, DepositId)
);