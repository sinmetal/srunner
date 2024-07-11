package balance

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/internal/trace"
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

func (v *UserDepositHistory) ToMutationMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["UserID"] = v.UserID
	m["DepositID"] = v.DepositID
	m["DepositType"] = v.DepositType.ToIntn()
	m["Amount"] = v.Amount
	m["Point"] = v.Point
	m["CreatedAt"] = v.CreatedAt
	return m
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
	ctx, _ = trace.StartSpan(ctx, "BalanceStore.Deposit")
	defer trace.EndSpan(ctx, err)

	var ub UserBalance
	var udh UserDepositHistory
	resp, err := s.sc.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var mus []*spanner.Mutation
		row, err := tx.ReadRowWithOptions(ctx, s.UserBalanceTable(),
			spanner.Key{userID},
			[]string{"UserID", "Amount", "Point", "CreatedAt", "UpdatedAt"},
			&spanner.ReadOptions{
				RequestTag: spanners.AppTag(),
			})
		if spanner.ErrCode(err) == codes.NotFound {
			ub = UserBalance{
				UserID:    userID,
				Amount:    amount,
				Point:     point,
				CreatedAt: spanner.CommitTimestamp,
			}
		} else if err != nil {
			return fmt.Errorf("failed read UserBalance : %w", err)
		} else {
			if err := row.ToStruct(&ub); err != nil {
				return fmt.Errorf("failed spanner.Row.ToStruct to UserBalance : %w", err)
			}
			ub.Amount += amount
			ub.Point += point
		}
		ub.UpdatedAt = spanner.CommitTimestamp
		ubMu, err := spanner.InsertOrUpdateStruct(s.UserBalanceTable(), ub)
		if err != nil {
			return fmt.Errorf("failed spanner.InsertOrUpdateStruct from UserBalance : %w", err)
		}
		mus = append(mus, ubMu)

		udh = UserDepositHistory{
			UserID:      userID,
			DepositID:   depositID,
			DepositType: depositType,
			Amount:      amount,
			Point:       point,
			CreatedAt:   spanner.CommitTimestamp,
		}
		udhMu := spanner.InsertMap(s.UserDepositHistoryTable(), udh.ToMutationMap())
		mus = append(mus, udhMu)

		if err := tx.BufferWrite(mus); err != nil {
			return fmt.Errorf("failed tx.BufferWrite : %w", err)
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

	return &ub, &udh, nil
}

func CreateUserID(ctx context.Context, id int64) string {
	return fmt.Sprintf("u%010d", id)
}

func RandomUserID(ctx context.Context) string {
	v := rand.Int63n(UserAccountIDMax)
	if v == 0 {
		return CreateUserID(ctx, 1)
	}
	return CreateUserID(ctx, v)
}

func CreateDepositID(ctx context.Context) string {
	return fmt.Sprintf("Deposit:%s", uuid.New().String())
}

func RandomDepositType(ctx context.Context) DepositType {
	i := rand.Intn(10)
	switch {
	case i == 8:
		return DepositTypeCampaignPoint
	case i == 5:
		return DepositTypeRefund
	case i == 4:
		return DepositTypeSales
	default:
		return DepositTypeBank
	}
}
