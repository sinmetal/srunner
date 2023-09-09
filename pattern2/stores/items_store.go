package stores

import "time"

type Item struct {
	ItemID    string
	ItemName  string
	Price     int64
	CreatedAt time.Time
	UpdatedAt time.Time
}
