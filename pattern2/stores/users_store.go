package stores

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

type UsersStore struct {
	sc *spanner.Client
}

func NewUsersStore(sc *spanner.Client) (*UsersStore, error) {
	return &UsersStore{
		sc: sc,
	}, nil
}

type User struct {
	UserID    string
	UserName  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (u *User) ToInsertMap() map[string]interface{} {
	m := map[string]interface{}{
		"UserID":    u.UserID,
		"UserName":  u.UserName,
		"CreatedAt": spanner.CommitTimestamp,
		"UpdatedAt": spanner.CommitTimestamp,
	}
	return m
}

func (s *UsersStore) TableName() string {
	return "Users"
}

func (s *UsersStore) Insert(ctx context.Context, user *User) (time.Time, error) {
	m := spanner.InsertMap(s.TableName(), user.ToInsertMap())
	var ms = []*spanner.Mutation{m}
	commitTimestamp, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite(ms)
	})
	if err != nil {
		return time.Time{}, err
	}
	return commitTimestamp, err
}
