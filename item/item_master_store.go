package item

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

type ItemMaster struct {
	ItemID     string
	Name       string
	Price      int64
	CommitedAt time.Time
}

type ItemMasterStore struct {
	sc *spanner.Client
}

func NewItemMasterStore(ctx context.Context, sc *spanner.Client) *ItemMasterStore {
	return &ItemMasterStore{sc: sc}
}

func (s *ItemMasterStore) TableName() string {
	return "ItemMaster"
}

func (s *ItemMasterStore) InsertOrUpdate(ctx context.Context, item *ItemMaster) error {
	m, err := spanner.InsertOrUpdateStruct(s.TableName(), item)
	if err != nil {
		return err
	}
	_, err = s.sc.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return err
	}
	return nil
}

func (s *ItemMasterStore) BatchInsertOrUpdate(ctx context.Context, items []*ItemMaster) error {
	var ms = make([]*spanner.Mutation, 0, len(items))
	for _, item := range items {
		m, err := spanner.InsertOrUpdateStruct(s.TableName(), item)
		if err != nil {
			return err
		}
		ms = append(ms, m)
	}

	_, err := s.sc.Apply(ctx, ms)
	if err != nil {
		return err
	}
	return nil
}
