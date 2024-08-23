package operation

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/internal/trace"
)

// OperationTableName is Operation Table Name
const OperationTableName = "Operation"

// Operation is Spanner Operation
type Operation struct {
	OperationID   string
	OperationName string
	ElapsedTimeMS int64
	Note          spanner.NullJSON
	CommitedAt    time.Time
}

type Store struct {
	sc *spanner.Client
}

func NewStore(ctx context.Context, sc *spanner.Client) (*Store, error) {
	return &Store{
		sc: sc,
	}, nil
}

func (s *Store) Insert(ctx context.Context, value *Operation) (ope *Operation, err error) {
	ctx, _ = trace.StartSpan(ctx, "OperationStore.Insert")
	defer trace.EndSpan(ctx, err)

	om, err := spanner.InsertStruct(OperationTableName, value)
	if err != nil {
		return nil, fmt.Errorf("failed Operation.Insert :%w", err)
	}
	ct, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{om})
	})
	value.CommitedAt = ct
	return value, nil
}
