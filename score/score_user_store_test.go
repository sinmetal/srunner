package score_test

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/score"
)

func newTestScoreUserStore(t *testing.T, sc *spanner.Client) *score.ScoreUserStore {
	ctx := context.Background()
	sus, err := score.NewScoreUserStore(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	return sus
}
