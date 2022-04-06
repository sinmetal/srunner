package balance_test

import (
	"context"
	"testing"

	"github.com/sinmetal/srunner/balance"
	"github.com/sinmetal/srunner/spannertest"
)

func TestStore_Deposit(t *testing.T) {
	ctx := context.Background()

	spannertest.IsSpannerEmulatorHost(t)

	sc, teardown := SetupSpannerTest(t, spannertest.RandomDatabaseName())
	defer teardown()

	bs, err := balance.NewStore(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		_, _, err = bs.Deposit(ctx, "u100", 100, 100)
		if err != nil {
			t.Fatal(err)
		}
	}
}
