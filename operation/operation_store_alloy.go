package operation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sinmetal/srunner/internal/trace"
)

type StoreAlloy struct {
	pool *pgxpool.Pool
}

type OperationAlloy struct {
	OperationID   string
	OperationName string
	ElapsedTimeMS int64
	Note          string
	CommitedAt    time.Time
}

func NewStoreAlloy(pool *pgxpool.Pool) *StoreAlloy {
	return &StoreAlloy{pool: pool}
}

func (s *StoreAlloy) OperationTable() string {
	return "Operation"
}

func (s *StoreAlloy) Insert(ctx context.Context, value *OperationAlloy) (ope *OperationAlloy, err error) {
	ctx, _ = trace.StartSpan(ctx, "OperationStoreAlloy.Insert")
	defer trace.EndSpan(ctx, err)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			if err2 := tx.Rollback(ctx); err2 != nil {
				if errors.Is(err2, pgx.ErrTxClosed) {
					return
				}
			}
		}
	}()

	insertOperationSQL :=
		fmt.Sprintf("INSERT INTO %s (OperationID, OperationName, ElapsedTimeMS, Note)"+
			" VALUES (@OperationID, @OperationName, @ElapsedTimeMS, @Note)",
			s.OperationTable(),
		)
	_, err = tx.Exec(ctx, insertOperationSQL,
		pgx.NamedArgs{
			"OperationID":   value.OperationID,
			"OperationName": value.OperationName,
			"ElapsedTimeMS": value.ElapsedTimeMS,
			"Note":          value.Note,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("insert operation: %w", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit operation: %w", err)
	}
	return value, nil
}
