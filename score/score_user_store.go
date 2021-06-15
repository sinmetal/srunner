package score

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
)

type ScoreUserStore struct {
	sc *spanner.Client
}

func NewScoreUserStore(ctx context.Context, sc *spanner.Client) (*ScoreUserStore, error) {
	return &ScoreUserStore{
		sc: sc,
	}, nil
}

type ScoreUser struct {
	ID         string `spanner:"Id"`
	CommitedAt time.Time
}

func (s *ScoreUserStore) ID(ctx context.Context, id int64) string {
	return fmt.Sprintf("user%08d", id)
}

func (s *ScoreUserStore) InsertBatch(ctx context.Context, ids []string) error {
	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var ms []*spanner.Mutation
		for _, id := range ids {
			e := &ScoreUser{
				ID:         id,
				CommitedAt: spanner.CommitTimestamp,
			}
			m, err := spanner.InsertStruct("ScoreUser", e)
			if err != nil {
				return err
			}
			ms = append(ms, m)
		}
		return tx.BufferWrite(ms)
	})
	if err != nil {
		return err
	}
	return nil
}
