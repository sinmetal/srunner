package score_test

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/score"
	"github.com/sinmetal/srunner/spannertest"
)

func TestScoreStore_Upsert(t *testing.T) {
	ctx := context.Background()
	sc, teardown := SetupSpannerTest(t, spannertest.RandomDatabaseName())
	defer teardown()

	ss := newTestScoreStore(t, sc)
	sus := newTestScoreUserStore(t, sc)

	// 新規作成
	const firstScore = int64(100)
	userID := sus.ID(ctx, 1)
	circleID := ss.CircleID(1)
	err := ss.Upsert(ctx, &score.Score{
		ID:         userID,
		Score:      firstScore,
		CircleID:   circleID,
		CommitedAt: spanner.CommitTimestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err := ss.Get(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := firstScore, v.Score; e != g {
		t.Errorf("want Score %d but got %d", e, g)
	}
	if e, g := firstScore, v.MaxScore; e != g {
		t.Errorf("want MaxScore %d but got %d", e, g)
	}
	if e, g := int64(0), v.ClassRank; e != g {
		t.Errorf("want ClassRank %d but got %d", e, g)
	}
	if e, g := circleID, v.CircleID; e != g {
		t.Errorf("want CircleID %s but got %s", e, g)
	}
	if v.CommitedAt.IsZero() {
		t.Errorf("CommitedAt is Zero")
	}

	// 低いスコアに更新
	const secondScore = int64(10)
	err = ss.Upsert(ctx, &score.Score{
		ID:         userID,
		Score:      secondScore,
		CircleID:   ss.CircleID(2),
		CommitedAt: spanner.CommitTimestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err = ss.Get(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := secondScore, v.Score; e != g {
		t.Errorf("want Score %d but got %d", e, g)
	}
	// MaxScoreは変わってないはず
	if e, g := firstScore, v.MaxScore; e != g {
		t.Errorf("want MaxScore %d but got %d", e, g)
	}
	if e, g := int64(0), v.ClassRank; e != g {
		t.Errorf("want ClassRank %d but got %d", e, g)
	}
	// CircleIDは最初のものが入っているはず
	if e, g := circleID, v.CircleID; e != g {
		t.Errorf("want CircleID %s but got %s", e, g)
	}

	// 高いスコアに更新
	const thirdScore = int64(10000000000)
	err = ss.Upsert(ctx, &score.Score{
		ID:         userID,
		Score:      thirdScore,
		CommitedAt: spanner.CommitTimestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err = ss.Get(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := thirdScore, v.Score; e != g {
		t.Errorf("want Score %d but got %d", e, g)
	}
	// MaxScore更新
	if e, g := thirdScore, v.MaxScore; e != g {
		t.Errorf("want MaxScore %d but got %d", e, g)
	}
	// ClassRank更新
	if e, g := int64(6), v.ClassRank; e != g {
		t.Errorf("want ClassRank %d but got %d", e, g)
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
