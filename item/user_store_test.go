package item

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

func TestUserStore_Insert(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	us := newTestCreateUserStore(t)

	const lot = 1000
	var count int
	var users = make([]*User, 0, lot)
	for i := 0; i < 10000000; i++ {
		count++
		users = append(users, &User{
			UserID:     fmt.Sprintf("%07d", i),
			Name:       uuid.New().String(),
			CommitedAt: spanner.CommitTimestamp,
		})
		if count >= lot {
			if err := us.BatchInsertOrUpdate(ctx, users); err != nil {
				t.Fatal(err)
			}
			count = 0
			users = make([]*User, 0, lot)
		}
	}
}

func newTestCreateUserStore(t *testing.T) *UserStore {
	ctx := context.Background()

	sc := newTestSpannerClient(t)
	return NewUserStore(ctx, sc)
}
