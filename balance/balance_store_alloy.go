package balance

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sinmetal/srunner/internal/trace"
)

type StoreAlloy struct {
	pool            *pgxpool.Pool
	readReplicaPool []*pgxpool.Pool
}

func NewStoreAlloy(pool *pgxpool.Pool, readReplicaPool []*pgxpool.Pool) *StoreAlloy {
	return &StoreAlloy{
		pool:            pool,
		readReplicaPool: readReplicaPool,
	}
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

// ReadUserBalances is 指定した複数のUserIDのBalanceを取得する
// defaultではReadReplicaから取得される。primary=trueにするとprimary instanceから取得される
func (s *StoreAlloy) ReadUserBalances(ctx context.Context, userIDs []string, primary bool) (models []*UserBalance, err error) {
	ctx, _ = trace.StartSpan(ctx, "BalanceStoreAlloy.ReadUserBalances")
	defer func() { trace.EndSpan(ctx, err) }()

	var pool *pgxpool.Pool
	if !primary && len(s.readReplicaPool) > 0 {
		pool = s.readReplicaPool[0]
	} else {
		pool = s.pool
	}
	placeholders := make([]string, len(userIDs))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	sql := fmt.Sprintf(`
SELECT UserID, Amount, Point FROM UserBalance WHERE UserID IN (%s)
`, strings.Join(placeholders, ","))
	//fmt.Println(sql)
	var args []any
	for _, userID := range userIDs {
		args = append(args, userID)
	}

	var results []*UserBalance
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("read user balances: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		columns, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read user balances: %w", err)
		}
		results = append(results, &UserBalance{
			UserID: columns[0].(string),
			Amount: columns[1].(int64),
			Point:  columns[2].(int64),
		})
	}
	return results, nil
}
