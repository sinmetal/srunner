package item

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

type ItemOrderDummyFK struct {
	ItemOrderID string
	ItemID      string
	UserID      string
	CommitedAt  time.Time
}

type ItemOrderDummyFKStore struct {
	sc *spanner.Client
}

func NewItemOrderDummyFKStore(ctx context.Context, sc *spanner.Client) *ItemOrderDummyFKStore {
	return &ItemOrderDummyFKStore{sc: sc}
}

func (s *ItemOrderDummyFKStore) TableName() string {
	return "ItemOrderDummyFK"
}

func (s *ItemOrderDummyFKStore) Insert(ctx context.Context, order *ItemOrderDummyFK) error {
	ctx, span := startSpan(ctx, "itemOrderDummyFK/insert")
	defer span.End()

	m, err := spanner.InsertStruct(s.TableName(), order)
	if err != nil {
		return err
	}
	_, err = s.sc.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return err
	}
	return nil
}
