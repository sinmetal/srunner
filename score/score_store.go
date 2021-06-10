package score

import (
	"context"
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
		var maxScore int64
		row, err := tx.ReadRow(ctx, "Score", spanner.Key{e.ID}, []string{"Id", "MaxScore"})
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				// Rowがない場合はMaxScoreはZeroと考える
			} else {
				return err
			}
		} else {
			ce := &Score{}
			if err := row.ToStruct(ce); err != nil {
				return err
			}
			maxScore = ce.MaxScore
		}

		if e.Score > maxScore {
			maxScore = e.Score
		}

		m := spanner.InsertOrUpdateMap("Score", map[string]interface{}{
			"Id":         e.ID,
			"CircleId":   e.CircleID,
			"Score":      e.Score,
			"MaxScore":   maxScore,
			"CommitedAt": spanner.CommitTimestamp,
		})
		return tx.BufferWrite([]*spanner.Mutation{m})
	})
	if err != nil {
		return err
	}
	return nil
}
