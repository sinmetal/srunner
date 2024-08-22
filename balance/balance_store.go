package balance

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/internal/trace"
	"github.com/sinmetal/srunner/spanners"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

var userAccountIDMax int64 = 1000000

func UserAccountIDMax() int64 {
	return userAccountIDMax
}

// SetUserAccountIDMax is default値から変更する場合、アプリケーション起動時に一度だけ呼ぶ
func SetUserAccountIDMax(v int64) {
	userAccountIDMax = v
}

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
	UserID                       string
	DepositID                    string
	DepositType                  DepositType `spanner:"-"`
	Amount                       int64
	Point                        int64
	SupplementaryInformationJson spanner.NullJSON
	SupplementaryInformation     *SupplementaryInformation `spanner:"-"`
	CreatedAt                    time.Time
}

type SupplementaryInformation struct {
	Name   spanner.NullString   `json:"name"`
	Rating spanner.NullFloat64  `json:"rating"`
	Open   interface{}          `json:"open"`
	Tags   []spanner.NullString `json:"tags"`
}

func (v *UserDepositHistory) FromRow(row *spanner.Row) (*UserDepositHistory, error) {
	ret := &UserDepositHistory{}

	if err := row.ColumnByName("UserID", &ret.UserID); err != nil {
		return nil, err
	}
	if err := row.ColumnByName("DepositID", &ret.DepositID); err != nil {
		return nil, err
	}
	var retDepositType int64
	if err := row.ColumnByName("DepositType", &retDepositType); err != nil {
		return nil, err
	}
	ret.DepositType = DepositType(retDepositType)

	if err := row.ColumnByName("Amount", &ret.Amount); err != nil {
		return nil, err
	}
	if err := row.ColumnByName("Point", &ret.Point); err != nil {
		return nil, err
	}
	return ret, nil
}

func (v *UserDepositHistory) ToMutationMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["UserID"] = v.UserID
	m["DepositID"] = v.DepositID
	m["DepositType"] = v.DepositType.ToIntn()
	m["Amount"] = v.Amount
	m["Point"] = v.Point
	m["SupplementaryInformation"] = v.SupplementaryInformationJson
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

		info := SupplementaryInformation{
			Name: spanner.NullString{
				StringVal: uuid.New().String(),
				Valid:     true,
			},
			Rating: spanner.NullFloat64{
				Float64: rand.Float64(),
				Valid:   true,
			},
			Open: RandomOpen(),
			Tags: RandomTags(),
		}
		infoJson := spanner.NullJSON{
			Value: info,
			Valid: true,
		}
		udh = UserDepositHistory{
			UserID:                       userID,
			DepositID:                    depositID,
			DepositType:                  depositType,
			Amount:                       amount,
			Point:                        point,
			SupplementaryInformationJson: infoJson,
			CreatedAt:                    spanner.CommitTimestamp,
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

func (s *Store) DepositDML(ctx context.Context, userID string, depositID string, depositType DepositType, amount int64, point int64) (userBalance *UserBalance, userDepositHistories *UserDepositHistory, err error) {
	ctx, _ = trace.StartSpan(ctx, "BalanceStore.DepositDML")
	defer trace.EndSpan(ctx, err)

	var ub UserBalance
	var udh *UserDepositHistory
	_, err = s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		insertDepositHistory := spanner.NewStatement(
			fmt.Sprintf("INSERT %s (UserID, DepositID, DepositType, Amount, Point, CreatedAt)"+
				" VALUES (@UserID, @DepositID, @DepositType, @Amount, @Point, PENDING_COMMIT_TIMESTAMP())"+
				" THEN RETURN UserID, DepositID, DepositType, Amount, Point", s.UserDepositHistoryTable()),
		)
		insertDepositHistory.Params = map[string]interface{}{
			"UserID":      userID,
			"DepositID":   depositID,
			"DepositType": depositType.ToIntn(),
			"Amount":      amount,
			"Point":       point,
		}
		iter := tx.Query(ctx, insertDepositHistory)
		for {
			row, err := iter.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return fmt.Errorf("failed Insert to DepositHistory: %w", err)
			}
			if udh, err = udh.FromRow(row); err != nil {
				return fmt.Errorf("failed Insert to DepositHistory result to Struct: %w", err)
			}
		}

		updateUserBalance := spanner.NewStatement(
			fmt.Sprintf("UPDATE %s SET Amount = Amount + @Amount, Point = Point + @Point, UpdatedAt = PENDING_COMMIT_TIMESTAMP()"+
				" WHERE UserID = @UserID"+
				" THEN RETURN UserID, Amount, Point", s.UserBalanceTable()),
		)
		updateUserBalance.Params = map[string]interface{}{
			"UserID": userID,
			"Amount": amount,
			"Point":  point,
		}
		iter = tx.Query(ctx, updateUserBalance)
		for {
			row, err := iter.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return fmt.Errorf("failed Update to UserBalance: %w", err)
			}
			if err := row.ToStruct(&ub); err != nil {
				return fmt.Errorf("failed Update to UserBalance result to Struct: %w", err)
			}
		}

		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return &ub, udh, err
}

func CreateUserID(ctx context.Context, id int64) string {
	return fmt.Sprintf("u%010d", id)
}

func RandomUserID(ctx context.Context) string {
	v := rand.Int63n(UserAccountIDMax())
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

func RandomOpen() map[string]bool {
	m := map[string]bool{}
	count := rand.Intn(7)
	for i := 0; count > i; i++ {
		n := rand.Intn(10)
		switch n {
		case 0:
			m["monday"] = true
		case 1:
			m["tuesday"] = true
		case 2:
			m["wednesday"] = true
		case 3:
			m["thursday"] = true
		case 4:
			m["friday"] = true
		case 5:
			m["saturday"] = true
		case 6:
			m["sunday"] = true
		default:
		}
	}
	return m
}

func RandomTags() []spanner.NullString {
	m := map[string]bool{}
	count := rand.Intn(10)
	for i := 0; count > i; i++ {
		n := rand.Intn(9)
		switch n {
		case 1:
			m["ComputeEngine"] = true
		case 2:
			m["CloudRun"] = true
		case 3:
			m["CloudSpanner"] = true
		case 4:
			m["BigQuery"] = true
		case 5:
			m["AppEngine"] = true
		case 6:
			m["CloudFirestore"] = true
		case 7:
			m["Dataflow"] = true
		default:
			m["ComputeEngine"] = true
			m["CloudStorage"] = true
		}
	}

	var tags []spanner.NullString
	for k, _ := range m {
		tags = append(tags, spanner.NullString{StringVal: k, Valid: true})
	}
	return tags
}
