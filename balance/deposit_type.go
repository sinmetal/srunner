package balance

type DepositType int

const (
	DepositTypeBank DepositType = iota
	DepositTypeCampaignPoint
	DepositTypeRefund
	DepositTypeSales
)
