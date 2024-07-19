package balance

import (
	"context"
	"fmt"
	"math/rand"
)

type Runner struct {
	BalanceStore *Store
}

func (r *Runner) Run(ctx context.Context) error {
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
	_, _, err := r.BalanceStore.Deposit(ctx, userAccountID, depositID, depositType, amount, point)
	if err != nil {
		return fmt.Errorf("failed balance.Depoist err=%s\n", err)
	}
	return nil
}
