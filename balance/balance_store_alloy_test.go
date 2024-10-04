package balance

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"testing"
	"time"

	"github.com/sinmetal/srunner/internal/alloy"
	"github.com/sinmetal/srunner/internal/trace"
)

// Userをいっぱい作るのに使っている。
func TestStoreAlloy_InsertUserBalance(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	user := "sinmetal"
	instanceName := "projects/kouzoh-p-sinmetal/locations/asia-northeast1/clusters/playground/instances/playground-primary"
	password := "hellloallloy"
	pgxCon, cleanup, err := alloy.ConnectPgx(ctx, instanceName, user, password, "quickstart_db")
	if err != nil {
		panic(err)
	}
	defer pgxCon.Close()
	defer func() {
		if err := cleanup(); err != nil {
			panic(fmt.Errorf("failed cleanup : %w", err))
		}
	}()
	func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := pgxCon.Ping(ctx); err != nil {
			panic(fmt.Errorf("failed ping : %w", err))
		}
	}()

	trace.Init(ctx, "test", "test")

	s := NewStoreAlloy(pgxCon, nil)
	for i := 491065; int64(i) < UserAccountIDMax(); i++ {
		userID := CreateUserID(ctx, int64(i))
		err := s.InsertUserBalance(ctx, &UserBalance{
			UserID: userID,
			Amount: 0,
			Point:  0,
		})
		if err != nil {
			var pgxErr *pgconn.PgError
			if errors.As(err, &pgxErr) {
				if pgxErr.Code == "23505" {
					// 重複した場合はSkip
					continue
				}
			}
			panic(err)
		}
	}
}
