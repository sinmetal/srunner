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

/*
2つのgoroutineがLockがぶつかるように実行して、片方がcontextをcancelしてから、11sec後に逆側のgoroutineが完遂した

=== RUN   TestLockCancel
18:44:21-goroutine1:start
18:44:22-goroutine2:start
18:44:26-goroutine1:failed failed Transaction: spanner: code = "DeadlineExceeded", desc = "context deadline exceeded, transaction outcome unknown"

18:44:40-goroutine2:mutationCount 11247
18:44:40-goroutine2:end
--- PASS: TestLockCancel (19.31s)
PASS
*/
func TestLockCancel(t *testing.T) {
	ctx := context.Background()

	dbName := os.Getenv("SRUNNER_SPANNER_DATABASE_NAME")
	if dbName == "" {
		t.Fatal("required $SRUNNER_SPANNER_DATABASE_NAME not set")
	}
	fmt.Printf("Target DB %s\n", dbName)
	cli1, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}

	bs1, err := balance.NewStore(ctx, cli1)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	cli2, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}

	bs2, err := balance.NewStore(ctx, cli2)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	c := make(chan error)
	const userID = "u0000000001"
	go func() {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second) // すぐtimeoutで死ぬ
		defer cancel()

		const routineName = "goroutine1"
		logTime(routineName, "start")
		resp, err := updateSumAmountPointHistoryV2(ctx, bs1, cli1, routineName, userID, 4*time.Second)
		if err != nil {
			logTime(routineName, fmt.Sprintf("failed %s", err))
			c <- err
			return
		}
		logTime(routineName, fmt.Sprintf("mutationCount %d", resp.CommitStats.GetMutationCount()))
		logTime(routineName, "end")
		c <- nil
	}()

	go func() {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		time.Sleep(1 * time.Second) // 後から追いかけていく

		const routineName = "goroutine2"
		logTime(routineName, "start")
		resp, err := updateSumAmountPointHistoryV2(ctx, bs2, cli2, routineName, userID, 0)
		if err != nil {
			logTime(routineName, fmt.Sprintf("failed %s", err))
			c <- err
			return
		}
		logTime(routineName, fmt.Sprintf("mutationCount %d", resp.CommitStats.GetMutationCount()))
		logTime(routineName, "end")
		c <- nil
	}()

	fmt.Println("wait")
	_ = <-c
	_ = <-c
}

func updateSumAmountPointHistory(ctx context.Context, bs *balance.Store, cli *spanner.Client, workerName string, userID string, wait time.Duration) (resp *spanner.CommitResponse, err error) {
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
	res, err := cli.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		iter := tx.QueryWithStats(ctx, stm)
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
	}, spanner.TransactionOptions{
		CommitOptions: spanner.CommitOptions{
			ReturnCommitStats: true,
		},
		TransactionTag:              "",
		CommitPriority:              0,
		ReadLockMode:                0,
		ExcludeTxnFromChangeStreams: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed Transaction: %s\n", err)
	}
	return &res, nil
}

// updateSumAmountPointHistoryV2 is 集計したUserDepositHistoryにチェックを入れていくバージョン
func updateSumAmountPointHistoryV2(ctx context.Context, bs *balance.Store, cli *spanner.Client, workerName string, userID string, wait time.Duration) (resp *spanner.CommitResponse, err error) {
	sql := `
SELECT
  DepositID, UserID, Amount, Point
FROM
  UserDepositHistory
WHERE UserID = @UserID
`
	sumVersion := time.Now().Format("2006-0102-15:04:05")
	stm := spanner.NewStatement(sql)
	stm.Params = map[string]interface{}{"UserID": userID}
	res, err := cli.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var amount int64
		var point int64
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
			amount += v.Amount
			point += v.Point
			m := spanner.UpdateMap("UserDepositHistory", map[string]interface{}{
				"UserID":     v.UserID,
				"DepositID":  v.DepositID,
				"SumVersion": sumVersion,
			})
			if err := tx.BufferWrite([]*spanner.Mutation{m}); err != nil {
				return fmt.Errorf("failed BufferWrite : %w", err)
			}
		}
		err := bs.InsertOrUpdateUserDepositHistorySum(ctx, tx, &balance.UserDepositHistorySum{
			UserID:    userID,
			Amount:    amount,
			Point:     point,
			Note:      workerName,
			UpdatedAt: spanner.CommitTimestamp,
		})
		if err != nil {
			return fmt.Errorf("failed InsertUserDepositHistorySum : %w", err)
		}
		time.Sleep(wait)
		return nil
	}, spanner.TransactionOptions{
		CommitOptions: spanner.CommitOptions{
			ReturnCommitStats: true,
		},
		TransactionTag:              "",
		CommitPriority:              0,
		ReadLockMode:                0,
		ExcludeTxnFromChangeStreams: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed Transaction: %s\n", err)
	}
	return &res, nil
}

func logTime(workerName string, funcName string) {
	fmt.Printf("%s-%s:%s\n", time.Now().Format("15:04:05"), workerName, funcName)
}
