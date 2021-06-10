package score_test

import (
	"context"
	"math/rand"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/score"
	"github.com/sinmetal/srunner/spannertest"
)

func TestScoreStore_Upsert(t *testing.T) {
	ctx := context.Background()
	sc, teardown := SetupSpannerTest(t, spannertest.RandomDatabaseName())
	defer teardown()

	ss := newTestScoreStore(t, sc)
	err := ss.Upsert(ctx, &score.Score{
		ID:         uuid.NewString(),
		Score:      rand.Int63(),
		CommitedAt: spanner.CommitTimestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func newTestScoreStore(t *testing.T, sc *spanner.Client) *score.ScoreStore {
	ctx := context.Background()

	ss, err := score.NewScoreStore(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	return ss
}
