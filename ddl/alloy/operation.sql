CREATE TABLE Operation (
    OperationId text NOT NULL,
    OperationName text NOT NULL,
    ElapsedTimeMS bigint NOT NULL,
    Note text,
    CommitedAt timestamp with time zone NOT NULL DEFAULT NOW(),
    PRIMARY KEY (OperationId)
);

CREATE INDEX idx_operation_name_elapsed_time
ON Operation (
    OperationName, ElapsedTimeMS DESC
);
