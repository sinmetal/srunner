package balance

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/spanners"
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
	ub := &UserBalance{}
	var udh *UserDepositHistory
	resp, err := s.sc.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var mus []*spanner.Mutation
		row, err := tx.ReadRowWithOptions(ctx, s.UserBalanceTable(),
			spanner.Key{userID},
			[]string{"UserID", "Amount", "Point", "CreatedAt", "UpdatedAt"},
			&spanner.ReadOptions{
				RequestTag: spanners.AppTag(),
			})
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
			ub.Amount += amount
			ub.Point += point
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
	}, spanner.TransactionOptions{
		TransactionTag: spanners.AppTag(),
	})
	if err != nil {
		return nil, nil, err
	}
	ub.CreatedAt = resp.CommitTs
	udh.CreatedAt = resp.CommitTs

	return ub, udh, nil
}
