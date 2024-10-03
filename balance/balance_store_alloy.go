package balance

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sinmetal/srunner/internal/trace"
)

type StoreAlloy struct {
	pool *pgxpool.Pool
}

func NewStoreAlloy(pool *pgxpool.Pool) *StoreAlloy {
	return &StoreAlloy{pool: pool}
}

func (s *StoreAlloy) UserDepositHistoryTable() string {
	return "UserDepositHistory"
}

func (s *StoreAlloy) UserBalanceTable() string {
	return "UserBalance"
}

func (s *StoreAlloy) Deposit(ctx context.Context, userID string, depositID string, depositType DepositType, amount int64, point int64) (err error) {
	ctx, _ = trace.StartSpan(ctx, "BalanceStoreAlloy.Deposit")
	defer func() { trace.EndSpan(ctx, err) }()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			if err2 := tx.Rollback(ctx); err2 != nil {
				if errors.Is(err2, pgx.ErrTxClosed) {
					return
				}
			}
		}
	}()

	insertDepositHistorySQL :=
		fmt.Sprintf("INSERT INTO %s (UserID, DepositID, DepositType, Amount, Point)"+
			" VALUES (@UserID, @DepositID, @DepositType, @Amount, @Point)",
			s.UserDepositHistoryTable(),
		)
	//fmt.Println(insertDepositHistorySQL)
	_, err = tx.Exec(ctx, insertDepositHistorySQL,
		pgx.NamedArgs{
			"UserID":      userID,
			"DepositID":   depositID,
			"DepositType": depositType,
			"Amount":      amount,
			"Point":       point,
		},
	)
	if err != nil {
		return fmt.Errorf("insert deposit history: %w", err)
	}

	updateUserBalanceSQL := fmt.Sprintf("UPDATE %s SET Amount = Amount + @Amount, Point = Point + @Point, UpdatedAt = NOW()"+
		" WHERE UserID = @UserID", s.UserBalanceTable(),
	)
	//fmt.Println(updateUserBalanceSQL)
	_, err = tx.Exec(ctx, updateUserBalanceSQL,
		pgx.NamedArgs{
			"UserID":      userID,
			"DepositID":   depositID,
			"DepositType": depositType,
			"Amount":      amount,
			"Point":       point,
		},
	)
	if err != nil {
		return fmt.Errorf("update user balance: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit user balance: %w", err)
	}
	return nil
}

func (s *StoreAlloy) InsertUserBalance(ctx context.Context, model *UserBalance) (err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if err2 := tx.Rollback(ctx); err2 != nil {
				if errors.Is(err2, pgx.ErrTxClosed) {
					return
				}
			}
		}
	}()

	sql := `
INSERT INTO UserBalance (UserID, Amount, Point) VALUES
    (@UserID, @Amount, @Point);
`
	_, err = tx.Exec(ctx, sql,
		pgx.NamedArgs{
			"UserID": model.UserID,
			"Amount": model.Amount,
			"Point":  model.Point,
		},
	)
	if err != nil {
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}
