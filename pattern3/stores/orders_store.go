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
	UserID     string
	OrderID    string
	Amount     int64
	CommitedAt time.Time
}

func (o *Order) ToInsertMap() map[string]interface{} {
	m := map[string]interface{}{
		"UserID":     o.UserID,
		"OrderID":    o.OrderID,
		"Amount":     o.Amount,
		"CommitedAt": spanner.CommitTimestamp,
	}
	return m
}

type OrderDetail struct {
	UserID        string
	OrderID       string
	OrderDetailID int64
	ItemID        string
	Price         int64
	Quantity      int64
	CommitedAt    time.Time
}

func (od *OrderDetail) ToInsertMap() map[string]interface{} {
	m := map[string]interface{}{
		"UserID":        od.UserID,
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
	var ms = make([]*spanner.Mutation, len(details)+1)
	var amount int64
	for i, detail := range details {
		m := spanner.InsertMap(s.OrderDetailsTableName(), detail.ToInsertMap())
		ms[i+1] = m
		amount += detail.Price * detail.Quantity
	}
	order := &Order{
		UserID:  userID,
		OrderID: orderID,
		Amount:  amount,
	}
	ms[0] = spanner.InsertMap(s.OrderTableName(), order.ToInsertMap())

	commitTimestamp, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite(ms)
	})
	if err != nil {
		return time.Time{}, err
	}
	return commitTimestamp, err
}
