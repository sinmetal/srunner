package balance

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/sinmetal/srunner/operation"
)

type DepositAlloyRunner struct {
	Store          *StoreAlloy
	OperationStore *operation.StoreAlloy
}

func (r *DepositAlloyRunner) Run(ctx context.Context) error {
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
	if err := r.Store.Deposit(ctx, userAccountID, depositID, depositType, amount, point); err != nil {
		return fmt.Errorf("failed BalanceStore.Deposit %w", err)
	}
	elapsed := time.Since(start)
	_, err := r.OperationStore.Insert(ctx, &operation.OperationAlloy{
		OperationID:   uuid.New().String(),
		OperationName: "BalanceStore.Deposit",
		ElapsedTimeMS: elapsed.Milliseconds(),
		Note:          "",
	})
	if err != nil {
		return fmt.Errorf("failed OperationStore.Insert err=%s\n", err)
	}
	return nil
}

type ReadUserBalancesAlloyRunner struct {
	Store *StoreAlloy
}

func (r *ReadUserBalancesAlloyRunner) Run(ctx context.Context) error {
	m := make(map[string]bool)
	var userAccountIDs []string
	for {
		userAccountID := RandomUserID(ctx)
		_, ok := m[userAccountID]
		if ok {
			continue
		}
		userAccountIDs = append(userAccountIDs, userAccountID)
		m[userAccountID] = true
		if len(userAccountIDs) >= 100 {
			break
		}
	}

	_, err := r.Store.ReadUserBalances(ctx, userAccountIDs, false)
	if err != nil {
		return fmt.Errorf("failed ReadUserBalances %w", err)
	}
	return nil
}

type FindUserDepositHistoriesAlloyRunner struct {
	Store *StoreAlloy
}

func (r *FindUserDepositHistoriesAlloyRunner) Run(ctx context.Context) error {
	userAccountID := RandomUserID(ctx)
	_, err := r.Store.FindUserDepositHistories(ctx, userAccountID, false)
	if err != nil {
		return fmt.Errorf("failed FindUserDepositHistories %w", err)
	}
	return nil
}
