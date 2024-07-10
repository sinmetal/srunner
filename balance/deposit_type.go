package balance

type DepositType int

const (
	// DepositTypeBank is 銀行
	DepositTypeBank DepositType = iota

	// DepositTypeCampaignPoint is キャンペーンポイント
	DepositTypeCampaignPoint

	// DepositTypeRefund is 返金
	DepositTypeRefund

	// DepositTypeSales is 売上
	DepositTypeSales
)

func (t DepositType) ToIntn() int64 {
	return int64(t)
}
