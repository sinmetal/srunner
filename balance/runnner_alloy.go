package balance

import (
	"context"
	"fmt"
	"math/rand"
)

type DepositAlloyRunner struct {
	Store *StoreAlloy
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
	if err := r.Store.Deposit(ctx, userAccountID, depositID, depositType, amount, point); err != nil {
		return fmt.Errorf("failed BalanceStore.Deposit %w", err)
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

type FindUserDepositHistoriesRunner struct {
	Store *StoreAlloy
}

func (r *FindUserDepositHistoriesRunner) Run(ctx context.Context) error {
	userAccountID := RandomUserID(ctx)
	models, err := r.Store.FindUserDepositHistories(ctx, userAccountID, false)
	if err != nil {
		return fmt.Errorf("failed FindUserDepositHistories %w", err)
	}
	fmt.Printf("FindUserDepositHistories.len %d \n", len(models))
	return nil
}
