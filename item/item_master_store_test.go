package item

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

func TestItemMasterStore_Insert(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	ims := newTestCreateItemMasterStore(t)

	const lot = 1000
	var count int
	var items = make([]*ItemMaster, 0, lot)
	for i := 0; i < 10000000; i++ {
		count++
		items = append(items, &ItemMaster{
			ItemID:     fmt.Sprintf("%07d", i),
			Name:       uuid.New().String(),
			Price:      rand.Int63n(100000) + 1000,
			CommitedAt: spanner.CommitTimestamp,
		})
		if count >= lot {
			if err := ims.BatchInsertOrUpdate(ctx, items); err != nil {
				t.Fatal(err)
			}
			count = 0
			items = make([]*ItemMaster, 0, lot)
		}
	}
}

func newTestCreateItemMasterStore(t *testing.T) *ItemMasterStore {
	ctx := context.Background()

	sc := newTestSpannerClient(t)
	return NewItemMasterStore(ctx, sc)
}
