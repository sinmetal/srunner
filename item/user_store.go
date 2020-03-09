package item

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

type User struct {
	UserID     string
	Name       string
	CommitedAt time.Time
}

type UserStore struct {
	sc *spanner.Client
}

func NewUserStore(ctx context.Context, sc *spanner.Client) *UserStore {
	return &UserStore{sc: sc}
}

func (s *UserStore) TableName() string {
	return "User"
}

func (s *UserStore) BatchInsertOrUpdate(ctx context.Context, users []*User) error {
	var ms = make([]*spanner.Mutation, 0, len(users))
	for _, user := range users {
		m, err := spanner.InsertOrUpdateStruct(s.TableName(), user)
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
