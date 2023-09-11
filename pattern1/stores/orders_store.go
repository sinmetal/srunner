package stores

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

type OrdersStore struct {
	sc *spanner.Client
}

func NewOrdersStore(sc *spanner.Client) (*OrdersStore, error) {
	return &OrdersStore{
		sc: sc,
	}, nil
}

type Order struct {
	OrderID    string
	UserID     string
	Amount     int64
	CommitedAt time.Time
}

func (o *Order) ToInsertMap() map[string]interface{} {
	m := map[string]interface{}{
		"OrderID":    o.OrderID,
		"UserID":     o.UserID,
		"Amount":     o.Amount,
		"CommitedAt": spanner.CommitTimestamp,
	}
	return m
}

type OrderDetail struct {
	OrderID       string
	OrderDetailID int64
	ItemID        string
	Price         int64
	Quantity      int64
	CommitedAt    time.Time
}

func (od *OrderDetail) ToInsertMap() map[string]interface{} {
	m := map[string]interface{}{
		"OrderID":       od.OrderID,
		"OrderDetailID": od.OrderDetailID,
		"ItemID":        od.ItemID,
		"Price":         od.Price,
		"Quantity":      od.Quantity,
		"CommitedAt":    spanner.CommitTimestamp,
	}
	return m
}

func (s *OrdersStore) OrderTableName() string {
	return "Orders"
}

func (s *OrdersStore) OrderDetailsTableName() string {
	return "OrderDetails"
}

func (s *OrdersStore) Insert(ctx context.Context, userID string, orderID string, details []*OrderDetail) (time.Time, error) {
	var ms []*spanner.Mutation
	var amount int64
	for _, detail := range details {
		m := spanner.InsertMap(s.OrderDetailsTableName(), detail.ToInsertMap())
		ms = append(ms, m)
		amount += detail.Price * detail.Quantity
	}
	order := &Order{
		OrderID: orderID,
		UserID:  userID,
		Amount:  amount,
	}

	orderMutation := spanner.InsertMap(s.OrderTableName(), order.ToInsertMap())
	ms = append(ms, orderMutation)

	commitTimestamp, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite(ms)
	})
	if err != nil {
		return time.Time{}, err
	}
	return commitTimestamp, err
}
