package item

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
)

func newTestSpannerClient(t *testing.T) *spanner.Client {
	db := os.Getenv("SRUNNER_TEST_DB")

	ctx := context.Background()
	sc, err := spanner.NewClient(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}
