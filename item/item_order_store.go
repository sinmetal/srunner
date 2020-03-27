package item

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

type ItemOrder struct {
	ItemOrderID string
	ItemID      string
	UserID      string
	CommitedAt  time.Time
}

type ItemOrderStore struct {
	sc *spanner.Client
}

func NewItemOrderStore(ctx context.Context, sc *spanner.Client) *ItemOrderStore {
	return &ItemOrderStore{sc: sc}
}

func (s *ItemOrderStore) TableName() string {
	return "ItemOrder"
}

func (s *ItemOrderStore) Insert(ctx context.Context, order *ItemOrder) error {
	ctx, span := startSpan(ctx, "itemOrder/insert")
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
