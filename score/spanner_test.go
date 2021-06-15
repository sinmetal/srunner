package score_test

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/spannertest"
)

func newTestSpannerClient(t *testing.T, spannerDatabase string) *spanner.Client {
	ctx := context.Background()
	sc, err := spanner.NewClient(ctx, spannerDatabase)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func SetupSpannerTest(t *testing.T, databaseName string) (*spanner.Client, func()) {
	ctx := context.Background()
	statements := spannertest.ReadDDLFile(t, "../ddl/score.sql")
	spannerDatabase := spannertest.NewDatabase(t, spannerProjectID, spannerInstanceID, databaseName, statements)

	sc, err := spanner.NewClient(ctx, spannerDatabase)
	if err != nil {
		t.Fatal(err)
	}
	return sc, func() {
		sc.Close()
	}
}
