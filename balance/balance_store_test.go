package balance_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/k0kubun/pp"
	"github.com/sinmetal/srunner/balance"
	"github.com/sinmetal/srunner/internal/trace"
)

func TestStore_DepositDML(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	trace.Init(ctx, "unit-test", "v0.0.0")

	spannerProjectID := os.Getenv("SRUNNER_SPANNER_PROJECT_ID")
	spannerInstanceID := os.Getenv("SRUNNER_SPANNER_INSTANCE_ID")
	spannerDatabaseID := os.Getenv("SRUNNER_SPANNER_DATABASE_ID")

	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectID, spannerInstanceID, spannerDatabaseID)

	sCli, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatal(err)
	}
	s, err := balance.NewStore(ctx, sCli)
	if err != nil {
		t.Fatal(err)
	}

	depositID := balance.CreateDepositID(ctx)
	ub, udh, err := s.DepositDML(ctx, "u0000000001", depositID, balance.DepositTypeBank, 10000, 0)
	if err != nil {
		t.Fatal(err)
	}
	pp.Print(ub)
	pp.Print(udh)
}

func TestStore_SelectUserDepositHistory(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	trace.Init(ctx, "unit-test", "v0.0.0")

	spannerProjectID := os.Getenv("SRUNNER_SPANNER_PROJECT_ID")
	spannerInstanceID := os.Getenv("SRUNNER_SPANNER_INSTANCE_ID")
	spannerDatabaseID := os.Getenv("SRUNNER_SPANNER_DATABASE_ID")

	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectID, spannerInstanceID, spannerDatabaseID)

	sCli, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatal(err)
	}
	s, err := balance.NewStore(ctx, sCli)
	if err != nil {
		t.Fatal(err)
	}

	l, err := s.SelectUserDepositHistory(ctx, "u0000110640", 10)
	if err != nil {
		t.Fatal(err)
	}
	pp.Println(l)
}
