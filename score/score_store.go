package score

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

// ScoreStore is 固定行数で更新をひたすら行うTable
type ScoreStore struct {
	sc *spanner.Client
}

func NewScoreStore(ctx context.Context, sc *spanner.Client) (*ScoreStore, error) {
	return &ScoreStore{
		sc: sc,
	}, nil
}

type Score struct {
	ID         string `spanner:"Id"` // 0 ~ 10億
	ClassRank  int64  // 6:10億以上,5:1億以上,4:1000万以上,3:100万以上,2:10万以上,1:10000以上,0:10000未満
	CircleID   string `spanner:"CircleId"` // 所属しているサークル,100000種類ぐらい
	Score      int64
	MaxScore   int64 // 過去最高スコア
	CommitedAt time.Time
}

func (s *ScoreStore) CircleID(id int64) string {
	return fmt.Sprintf("circle%08d", id)
}

// InsertBatch is 最初に一気にデータを作るためのもの
func (s *ScoreStore) InsertBatch(ctx context.Context, ids []string) error {
	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var ms []*spanner.Mutation
		for _, id := range ids {
			m := spanner.InsertMap("Score", map[string]interface{}{
				"Id":         id,
				"CommitedAt": spanner.CommitTimestamp,
			})
			ms = append(ms, m)
		}
		return tx.BufferWrite(ms)
	})
	if err != nil {
		return err
	}
	return nil
}

// Upsert is Scoreを更新する
func (s *ScoreStore) Upsert(ctx context.Context, e *Score) error {
	ctx, span := startSpan(ctx, "ScoreStore/upsert")
	defer span.End()

	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var m *spanner.Mutation
		circleID := e.CircleID
		row, err := tx.ReadRow(ctx, "Score", spanner.Key{e.ID}, []string{"Id", "MaxScore"})
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				// Rowがない場合はMaxScoreはZeroと考える
				m = spanner.InsertMap("Score", map[string]interface{}{
					"Id":         e.ID,
					"CircleId":   circleID,
					"Score":      e.Score,
					"MaxScore":   e.Score,
					"CommitedAt": spanner.CommitTimestamp,
				})
			} else {
				return err
			}
		} else {
			ce := &Score{}
			if err := row.ToStruct(ce); err != nil {
				return err
			}
			maxScore := ce.MaxScore
			if e.Score > maxScore {
				maxScore = e.Score
			}
			m = spanner.UpdateMap("Score", map[string]interface{}{
				"Id":         e.ID,
				"Score":      e.Score,
				"MaxScore":   maxScore,
				"CommitedAt": spanner.CommitTimestamp,
			})
		}

		return tx.BufferWrite([]*spanner.Mutation{m})
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *ScoreStore) Get(ctx context.Context, id string) (*Score, error) {
	ctx, span := startSpan(ctx, "ScoreStore/get")
	defer span.End()

	row, err := s.sc.Single().ReadRow(ctx, "Score", spanner.Key{id},
		[]string{"Id", "ClassRank", "CircleId", "Score", "MaxScore", "CommitedAt"})
	if err != nil {
		return nil, err
	}
	var e Score
	if err := row.ToStruct(&e); err != nil {
		return nil, err
	}
	return &e, nil
}
