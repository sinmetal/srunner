package balance

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/operation"
)

type DepositRunner struct {
	BalanceStore   *Store
	OperationStore *operation.Store
}

func (r *DepositRunner) Run(ctx context.Context) error {
	userAccountID := RandomUserID(ctx)
	depositID := CreateDepositID(ctx)
	var amount int64
	var point int64
	depositType := RandomDepositType(ctx)
	switch depositType {
	case DepositTypeBank:
		switch rand.Intn(5) {
		case 1:
			amount = 10000
		case 2:
			amount = 20000
		case 3:
			amount = 30000
		default:
			amount = int64(1000 + rand.Intn(200000))
		}
	case DepositTypeCampaignPoint:
		point = int64(10 + rand.Intn(1000))
	case DepositTypeRefund:
		amount = int64(10 + rand.Intn(1000))
	case DepositTypeSales:
		amount = int64(500 + rand.Intn(10000))
		point = int64(500 + rand.Intn(10000))
	default:
		fmt.Println("unsupported DepositType")
		return nil
	}
	start := time.Now()
	_, _, err := r.BalanceStore.Deposit(ctx, userAccountID, depositID, depositType, amount, point)
	if err != nil {
		return fmt.Errorf("failed balance.Depoist err=%s\n", err)
	}
	elapsed := time.Since(start)
	_, err = r.OperationStore.Insert(ctx, &operation.Operation{
		OperationID:   uuid.New().String(),
		OperationName: "BalanceStore.Deposit",
		ElapsedTimeMS: elapsed.Milliseconds(),
		Note:          spanner.NullJSON{},
		CommitedAt:    spanner.CommitTimestamp,
	})
	if err != nil {
		return fmt.Errorf("failed OperationStore.Insert err=%s\n", err)
	}
	return nil
}

type DepositDMLRunner struct {
	BalanceStore   *Store
	OperationStore *operation.Store
}

func (r *DepositDMLRunner) Run(ctx context.Context) error {
	userAccountID := RandomUserID(ctx)
	depositID := CreateDepositID(ctx)
	var amount int64
	var point int64
	depositType := RandomDepositType(ctx)
	switch depositType {
	case DepositTypeBank:
		switch rand.Intn(5) {
		case 1:
			amount = 10000
		case 2:
			amount = 20000
		case 3:
			amount = 30000
		default:
			amount = int64(1000 + rand.Intn(200000))
		}
	case DepositTypeCampaignPoint:
		point = int64(10 + rand.Intn(1000))
	case DepositTypeRefund:
		amount = int64(10 + rand.Intn(1000))
	case DepositTypeSales:
		amount = int64(500 + rand.Intn(10000))
		point = int64(500 + rand.Intn(10000))
	default:
		fmt.Println("unsupported DepositType")
		return nil
	}

	start := time.Now()
	_, _, err := r.BalanceStore.DepositDML(ctx, userAccountID, depositID, depositType, amount, point)
	if err != nil {
		return fmt.Errorf("failed balance.DepositDML err=%s\n", err)
	}
	elapsed := time.Since(start)
	_, err = r.OperationStore.Insert(ctx, &operation.Operation{
		OperationID:   uuid.New().String(),
		OperationName: "BalanceStore.DepositDML",
		ElapsedTimeMS: elapsed.Milliseconds(),
		Note:          spanner.NullJSON{},
		CommitedAt:    spanner.CommitTimestamp,
	})
	if err != nil {
		return fmt.Errorf("failed OperationStore.Insert err=%s\n", err)
	}
	return nil
}

type FindUserDepositHistoriesRunner struct {
	BalanceStore *Store
}

func (r *FindUserDepositHistoriesRunner) Run(ctx context.Context) error {
	userID := RandomUserID(ctx)
	models, err := r.BalanceStore.FindUserDepositHistories(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed FindUserDepositHistories err=%s\n", err)
	}
	fmt.Printf("FindUserDepositHistories length %d\n", len(models))
	return nil
}
