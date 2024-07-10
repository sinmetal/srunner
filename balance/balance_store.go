package balance

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/spanners"
	"google.golang.org/grpc/codes"
)

const (
	UserAccountIDMax = 1000000
)

type Store struct {
	sc *spanner.Client
}

func NewStore(ctx context.Context, sc *spanner.Client) (*Store, error) {
	return &Store{
		sc: sc,
	}, nil
}

type UserAccount struct {
	UserID    string
	Age       int64
	Height    int64
	Weight    int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type UserBalance struct {
	UserID    string
	Amount    int64
	Point     int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type UserDepositHistory struct {
	UserID      string
	DepositID   string
	DepositType DepositType
	Amount      int64
	Point       int64
	CreatedAt   time.Time
}

func (s *Store) UserAccountTable() string {
	return "UserAccount"
}

func (s *Store) UserBalanceTable() string {
	return "UserBalance"
}

func (s *Store) UserDepositHistoryTable() string {
	return "UserDepositHistory"
}

func (s *Store) CreateUserAccount(ctx context.Context, userAccount *UserAccount) (resultUserAccount *UserAccount, err error) {
	userAccount.CreatedAt = spanner.CommitTimestamp
	userAccount.UpdatedAt = spanner.CommitTimestamp
	commitTime, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		m, err := spanner.InsertStruct(s.UserAccountTable(), userAccount)
		if err != nil {
			return err
		}
		if err := tx.BufferWrite([]*spanner.Mutation{m}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	userAccount.CreatedAt = commitTime
	userAccount.UpdatedAt = commitTime
	return userAccount, nil
}

func (s *Store) Deposit(ctx context.Context, userID string, depositID string, depositType DepositType, amount int64, point int64) (userBalance *UserBalance, userDepositHistories *UserDepositHistory, err error) {
	var ub *UserBalance
	var udh *UserDepositHistory
	resp, err := s.sc.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var mus []*spanner.Mutation
		row, err := tx.ReadRowWithOptions(ctx, s.UserBalanceTable(),
			spanner.Key{userID},
			[]string{"UserID", "DepositType", "Amount", "Point", "CreatedAt", "UpdatedAt"},
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
			UserID:      userID,
			DepositID:   depositID,
			DepositType: depositType,
			Amount:      amount,
			Point:       point,
			CreatedAt:   spanner.CommitTimestamp,
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
