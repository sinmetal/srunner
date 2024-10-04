package balance

import (
	"context"
	"encoding/json"
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
	UserID                   string
	DepositID                string
	DepositType              DepositType `spanner:"-"`
	Amount                   int64
	Point                    int64
	SumVersion               string
	SupplementaryInformation *SupplementaryInformation `spanner:"-"`
	CreatedAt                time.Time
}

type SupplementaryInformation struct {
	Name   string      `json:"name"`
	Rating float64     `json:"rating"`
	Open   interface{} `json:"open"`
	Tags   []string    `json:"tags"`
}

type UserDepositHistorySum struct {
	UserID    string
	Amount    int64
	Point     int64
	Count     int64
	Note      string
	UpdatedAt time.Time
}

func (v *UserDepositHistory) FromRow(row *spanner.Row) (*UserDepositHistory, error) {
	ret := &UserDepositHistory{}

	if err := row.ColumnByName("UserID", &ret.UserID); err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return nil, err
		}
	}
	if err := row.ColumnByName("DepositID", &ret.DepositID); err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return nil, err
		}
	}
	var retDepositType int64
	if err := row.ColumnByName("DepositType", &retDepositType); err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return nil, err
		}
	}
	ret.DepositType = DepositType(retDepositType)

	if err := row.ColumnByName("Amount", &ret.Amount); err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return nil, err
		}
	}
	if err := row.ColumnByName("Point", &ret.Point); err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return nil, err
		}
	}
	if err := row.ColumnByName("CreatedAt", &ret.CreatedAt); err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return nil, err
		}
	}

	var info spanner.NullJSON
	if err := row.ColumnByName("SupplementaryInformation", &info); err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return nil, err
		}
	}
	if info.Valid {
		var v SupplementaryInformation
		if err := json.Unmarshal([]byte(info.String()), &v); err != nil {
			return nil, fmt.Errorf("failed SupplementaryInformation.Unmarshal")
		}
		ret.SupplementaryInformation = &v
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

	if v.SupplementaryInformation != nil {
		j := spanner.NullJSON{
			Value: v.SupplementaryInformation,
			Valid: true,
		}
		m["SupplementaryInformation"] = j
	}

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
	defer func() { trace.EndSpan(ctx, err) }()

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
			Name:   uuid.New().String(),
			Rating: rand.Float64(),
			Open:   RandomOpen(),
			Tags:   RandomTags(),
		}

		udh = UserDepositHistory{
			UserID:                   userID,
			DepositID:                depositID,
			DepositType:              depositType,
			Amount:                   amount,
			Point:                    point,
			SupplementaryInformation: &info,
			CreatedAt:                spanner.CommitTimestamp,
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
	defer func() { trace.EndSpan(ctx, err) }()

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

func (s *Store) SelectUserDepositHistory(ctx context.Context, userID string, limit int) (list []*UserDepositHistory, err error) {
	ctx, _ = trace.StartSpan(ctx, "BalanceStore.SelectUserDepositHistory")
	defer func() { trace.EndSpan(ctx, err) }()

	const q = `
SELECT 
  UserID,
	DepositID,
	DepositType,
	Amount,
	Point,
	SupplementaryInformation,
	CreatedAt
FROM UserDepositHistory
WHERE UserID = @UserID
ORDER BY CreatedAt DESC
LIMIT @Limit
`

	stm := spanner.NewStatement(q)
	stm.Params = map[string]interface{}{
		"UserID": userID,
		"Limit":  limit,
	}

	iter := s.sc.Single().Query(ctx, stm)
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed SelectUserDepositHistory : %w", err)
		}
		v := &UserDepositHistory{}
		v, err = v.FromRow(row)
		if err != nil {
			return nil, fmt.Errorf("failed UserDepositHistory.FromRow : %w", err)
		}
		list = append(list, v)
	}
	return list, nil
}

// InsertOrUpdateUserDepositHistorySum is UserDepositHistorySum TableにInsertOrUpdateする
func (s *Store) InsertOrUpdateUserDepositHistorySum(ctx context.Context, tx *spanner.ReadWriteTransaction, value *UserDepositHistorySum) (err error) {
	row, err := tx.ReadRow(ctx, "UserDepositHistorySum", spanner.Key{value.UserID}, []string{"UserID", "Amount", "Point", "Count"})
	if err != nil {
		if errors.Is(err, spanner.ErrRowNotFound) {
			// noop
		} else {
			return fmt.Errorf("failed Insert to UserDepositHistorySum: %w", err)
		}
	}
	if row != nil {
		var count int64
		if err := row.ColumnByName("Count", &count); err != nil {
			return fmt.Errorf("failed Insert to UserDepositHistorySum result to Struct: %w", err)
		}
		value.Count = count + 1
	}

	m, err := spanner.InsertOrUpdateStruct("UserDepositHistorySum", value)
	if err != nil {
		return err
	}
	if err := tx.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return err
	}
	return nil
}

// FindUserDepositHistories is 指定したuserIDのUserDepositHistoryの最新100件を取得する
// SQLで最初から取得すれば良いが、GetMultiをやるめたにワンクッション置いている
func (s *Store) FindUserDepositHistories(ctx context.Context, userID string) (models []*UserDepositHistory, err error) {
	ctx, _ = trace.StartSpan(ctx, "BalanceStore.FindUserDepositHistories")
	defer func() { trace.EndSpan(ctx, err) }()

	var userDepositHistoryKeys []spanner.Key
	{
		stm := spanner.NewStatement("SELECT UserID, DepositID FROM UserDepositHistory WHERE UserID = @UserID ORDER BY CreatedAt DESC LIMIT 100")
		stm.Params = map[string]interface{}{"UserID": userID}
		iter := s.sc.Single().Query(ctx, stm)
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed FindUserDepositHistories : %w", err)
			}
			var userID string
			if err := row.ColumnByName("UserID", &userID); err != nil {
				return nil, fmt.Errorf("failed UserID ColumnByName : %w", err)
			}
			var depositID string
			if err := row.ColumnByName("DepositID", &depositID); err != nil {
				return nil, fmt.Errorf("failed DepositID ColumnByName : %w", err)
			}
			userDepositHistoryKeys = append(userDepositHistoryKeys, spanner.Key{userID, depositID})
		}
	}

	var results []*UserDepositHistory
	iter := s.sc.Single().Read(ctx, s.UserDepositHistoryTable(), spanner.KeySetFromKeys(userDepositHistoryKeys...),
		[]string{"UserID", "DepositID", "Amount", "Point"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed FindUserDepositHistories : %w", err)
		}
		var v UserDepositHistory
		if err := row.ToStruct(&v); err != nil {
			return nil, fmt.Errorf("failed row.Struct() FindUserDepositHistories : %w", err)
		}
		results = append(results, &v)
	}
	return results, nil
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

func RandomTags() []string {
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

	var tags []string
	for k, _ := range m {
		tags = append(tags, k)
	}
	return tags
}
