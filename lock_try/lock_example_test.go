package lock_try

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

const LockTryTableName = "LockTrys"

type LockTry struct {
	LockTryID string
	UserID    string
	Number    int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// TestLock
// Insertされたものが範囲に入るようなQueryを実行している場合、Lockが競合する
// 以下のケースだと、LockTry TableのInsertとUserIDを指定したSelectが競合する
// Userが異なれば、競合しない
func TestLock(t *testing.T) {
	ctx := context.Background()

	dbName := os.Getenv("SRUNNER_SPANNER_DATABASE_NAME")
	if dbName == "" {
		t.Fatal("required $SRUNNER_SPANNER_DATABASE_NAME not set")
	}
	cli, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		_, err = cli.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if err := InsertLockTry(ctx, tx, "1"); err != nil {
				return fmt.Errorf("could not insert lock try: %w", err)
			}
			time.Sleep(1 * time.Second)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed spanner.ReadWriteTransaction() LockTry: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		_, err = cli.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if err := InsertLockTry(ctx, tx, "2"); err != nil {
				return fmt.Errorf("could not insert lock try: %w", err)
			}
			time.Sleep(3 * time.Second)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed spanner.ReadWriteTransaction() LockTry: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		_, err = cli.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if err := InsertOrUpdateLockTry(ctx, tx, "1"); err != nil {
				return fmt.Errorf("could not insert or update lock try: %w", err)
			}
			time.Sleep(3 * time.Second)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed spanner.ReadWriteTransaction() LockTry: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		_, err = cli.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if err := InsertOrUpdateLockTry(ctx, tx, "2"); err != nil {
				return fmt.Errorf("could not insert or update lock try: %w", err)
			}
			time.Sleep(3 * time.Second)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed spanner.ReadWriteTransaction() LockTry: %w", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func InsertLockTry(ctx context.Context, tx *spanner.ReadWriteTransaction, userID string) error {
	var model *LockTry
	model = &LockTry{
		LockTryID: uuid.New().String(),
		UserID:    userID,
		Number:    0,
		CreatedAt: spanner.CommitTimestamp,
		UpdatedAt: spanner.CommitTimestamp,
	}
	mu, err := spanner.InsertStruct(LockTryTableName, model)
	if err != nil {
		return fmt.Errorf("failed spanner.InsertStruct() LockTry: %w", err)
	}
	if err := tx.BufferWrite([]*spanner.Mutation{mu}); err != nil {
		return fmt.Errorf("failed tx.BufferWrite() LockTry: %w", err)
	}
	return nil
}

func InsertOrUpdateLockTry(ctx context.Context, tx *spanner.ReadWriteTransaction, userID string) error {
	var models []*LockTry
	sts := spanner.NewStatement("SELECT * FROM LockTrys WHERE UserID = @UserID")
	sts.Params["UserID"] = userID
	iter := tx.Query(ctx, sts)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return err
		}
		var model LockTry
		if err := row.ToStruct(&model); err != nil {
			return err
		}
		models = append(models, &model)
	}

	var model *LockTry
	if len(models) > 0 {
		model = models[0]
	}
	if model == nil {
		model = &LockTry{
			LockTryID: uuid.New().String(),
			UserID:    userID,
			CreatedAt: spanner.CommitTimestamp,
		}
	}
	model.Number++
	model.UpdatedAt = spanner.CommitTimestamp
	mu, err := spanner.InsertOrUpdateStruct(LockTryTableName, model)
	if err != nil {
		return fmt.Errorf("failed spanner.UpdateStruct() LockTry: %w", err)
	}
	if err := tx.BufferWrite([]*spanner.Mutation{mu}); err != nil {
		return fmt.Errorf("failed tx.BufferWrite() LockTry: %w", err)
	}
	return nil
}
