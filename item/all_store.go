package item

import (
	"context"

	"cloud.google.com/go/spanner"
)

type AllStore struct {
	IMS    *ItemMasterStore
	IOS    *ItemOrderStore
	IOSFK  *ItemOrderNOFKStore
	IODFKS *ItemOrderDummyFKStore
	US     *UserStore
}

func NewAllStore(ctx context.Context, sc *spanner.Client) *AllStore {
	us := NewUserStore(ctx, sc)
	ims := NewItemMasterStore(ctx, sc)
	ios := NewItemOrderStore(ctx, sc)
	iosfk := NewItemOrderNOFKStore(ctx, sc)
	iodfks := NewItemOrderDummyFKStore(ctx, sc)

	return &AllStore{
		IMS:    ims,
		IOS:    ios,
		IOSFK:  iosfk,
		IODFKS: iodfks,
		US:     us,
	}
}
