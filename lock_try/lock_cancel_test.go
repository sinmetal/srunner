package lock_try

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/balance"
	"google.golang.org/api/iterator"
)

func TestLockCancel(t *testing.T) {
	ctx := context.Background()

	dbName := os.Getenv("SRUNNER_SPANNER_DATABASE_NAME")
	if dbName == "" {
		t.Fatal("required $SRUNNER_SPANNER_DATABASE_NAME not set")
	}
	fmt.Printf("Target DB %s\n", dbName)
	cli, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}

	bs, err := balance.NewStore(ctx, cli)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	c := make(chan error)
	const userID = "u0008900358"
	go func() {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second) // すぐtimeoutで死ぬ
		defer cancel()

		const routineName = "goroutine1"
		logTime(routineName, "start")
		if err := updateSumAmountPointHistory(ctx, bs, cli, routineName, userID, 3*time.Second); err != nil {
			logTime(routineName, fmt.Sprintf("failed %s", err))
			c <- err
			return
		}
		logTime(routineName, "end")
		c <- nil
	}()

	go func() {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		time.Sleep(1 * time.Second) // 後から追いかけていく

		const routineName = "goroutine2"
		logTime(routineName, "start")
		if err := updateSumAmountPointHistory(ctx, bs, cli, routineName, userID, 0); err != nil {
			logTime(routineName, fmt.Sprintf("failed %s", err))
			c <- err
			return
		}
		logTime(routineName, "end")
		c <- nil
	}()

	//go func() {
	//	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	//	defer cancel()
	//
	//	const routineName = "goroutine3"
	//	logTime(routineName, "start")
	//	if err := updateSumAmountPointHistory(ctx, bs, cli, routineName, userID, 2*time.Second); err != nil { // 結構待ってから最後に終わる
	//		logTime(routineName, fmt.Sprintf("failed %s", err))
	//		c <- err
	//		return
	//	}
	//	logTime(routineName, "end")
	//	c <- nil
	//}()

	fmt.Println("wait")
	_ = <-c
	_ = <-c
	//_ = <-c
}

func updateSumAmountPointHistory(ctx context.Context, bs *balance.Store, cli *spanner.Client, workerName string, userID string, wait time.Duration) error {
	sql := `
SELECT
  UserID, Sum(Amount) AS Amount, Sum(Point) AS Point
FROM
  UserDepositHistory
WHERE UserID = @UserID
GROUP BY UserID
`

	stm := spanner.NewStatement(sql)
	stm.Params = map[string]interface{}{"UserID": userID}
	_, err := cli.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		iter := tx.Query(ctx, stm)
		for {
			row, err := iter.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return fmt.Errorf("failed Query : %w", err)
			}
			var v balance.UserDepositHistory
			if err := row.ToStruct(&v); err != nil {
				return fmt.Errorf("failed ToStruct : %w", err)
			}

			err = bs.InsertOrUpdateUserDepositHistorySum(ctx, tx, &balance.UserDepositHistorySum{
				UserID:    userID,
				Amount:    v.Amount,
				Point:     v.Point,
				Note:      workerName,
				UpdatedAt: spanner.CommitTimestamp,
			})
			if err != nil {
				return fmt.Errorf("failed InsertUserDepositHistorySum : %w", err)
			}
		}
		time.Sleep(wait)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed Transaction: %s\n", err)
	}
	return nil
}

func logTime(workerName string, funcName string) {
	fmt.Printf("%s-%s:%s\n", time.Now().Format("15:04:05"), workerName, funcName)
}
