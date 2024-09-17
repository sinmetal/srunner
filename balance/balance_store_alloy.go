package balance

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type StoreAlloy struct {
	pool *pgxpool.Pool
}

func NewStoreAlloy(pool *pgxpool.Pool) *StoreAlloy {
	return &StoreAlloy{pool: pool}
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
