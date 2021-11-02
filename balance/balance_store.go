package balance

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
)

type Store struct {
	sc *spanner.Client
}

func NewStore(ctx context.Context, sc *spanner.Client) (*Store, error) {
	return &Store{
		sc: sc,
	}, nil
}

type UserBalance struct {
	UserID    string
	Amount    int64
	Point     int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type UserDepositHistory struct {
	DepositID string
	UserID    string
	Amount    int64
	Point     int64
	CreatedAt time.Time
}

func (s *Store) UserBalanceTable() string {
	return "UserBalance"
}

func (s *Store) UserDepositHistoryTable() string {
	return "UserDepositHistory"
}

func (s *Store) Deposit(ctx context.Context, userID string, amount int64, point int64) (userBalance *UserBalance, userDepositHistories *UserDepositHistory, err error) {
	var ub *UserBalance
	var udh *UserDepositHistory
	commitTime, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var mus []*spanner.Mutation
		row, err := tx.ReadRow(ctx, s.UserBalanceTable(), spanner.Key{userID}, []string{"UserID", "Amount", "Point", "CreatedAt", "UpdatedAt"})
		if spanner.ErrCode(err) == codes.NotFound {
			ub = &UserBalance{
				UserID:    userID,
				Amount:    amount,
				Point:     point,
				CreatedAt: spanner.CommitTimestamp,
			}
		} else if err != nil {
			return err
		} else {
			if err := row.ToStruct(ub); err != nil {
				return err
			}
		}
		ub.UpdatedAt = spanner.CommitTimestamp
		ubMu, err := spanner.InsertOrUpdateStruct(s.UserBalanceTable(), ub)
		if err != nil {
			return err
		}
		mus = append(mus, ubMu)

		udh = &UserDepositHistory{
			DepositID: uuid.New().String(),
			UserID:    userID,
			Amount:    amount,
			Point:     point,
			CreatedAt: spanner.CommitTimestamp,
		}
		udhMu, err := spanner.InsertStruct(s.UserDepositHistoryTable(), udh)
		if err != nil {
			return err
		}
		mus = append(mus, udhMu)

		if err := tx.BufferWrite(mus); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	ub.CreatedAt = commitTime
	udh.CreatedAt = commitTime

	return ub, udh, nil
}
