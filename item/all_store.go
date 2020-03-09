package item

import (
	"context"

	"cloud.google.com/go/spanner"
)

type AllStore struct {
	IMS *ItemMasterStore
	IOS *ItemOrderStore
	US  *UserStore
}

func NewAllStore(ctx context.Context, sc *spanner.Client) *AllStore {
	us := NewUserStore(ctx, sc)
	ims := NewItemMasterStore(ctx, sc)
	ios := NewItemOrderStore(ctx, sc)

	return &AllStore{
		IMS: ims,
		IOS: ios,
		US:  us,
	}
}
