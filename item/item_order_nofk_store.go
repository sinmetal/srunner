package item

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

type ItemOrderNOFK struct {
	ItemOrderID string
	ItemID      string
	UserID      string
	CommitedAt  time.Time
}

type ItemOrderNOFKStore struct {
	sc *spanner.Client
}

func NewItemOrderNOFKStore(ctx context.Context, sc *spanner.Client) *ItemOrderNOFKStore {
	return &ItemOrderNOFKStore{sc: sc}
}

func (s *ItemOrderNOFKStore) TableName() string {
	return "ItemOrderNOFK"
}

func (s *ItemOrderNOFKStore) Insert(ctx context.Context, order *ItemOrderNOFK) error {
	ctx, span := startSpan(ctx, "itemOrderNOFK/insert")
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
