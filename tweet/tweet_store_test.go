package tweet

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestDefaultTweetStore_Insert(t *testing.T) {
	ctx := context.Background()

	dbName := fmt.Sprintf("projects/fake/instances/fake/databases/%s", RandString(8))
	fmt.Println(dbName)
	sc, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	statements := readDDLFile(t, "../ddl/tweet.sql")
	ddl(t, dbName, statements)

	ts := NewTweetStore(sc)

	now := time.Now()
	err = ts.Insert(ctx, &Tweet{
		ID:             "test",
		Author:         "sinmetal",
		Content:        "hello world",
		Count:          0,
		Favos:          []string{},
		Sort:           0,
		ShardCreatedAt: 0,
		CreatedAt:      now,
		UpdatedAt:      now,
		CommitedAt:     spanner.CommitTimestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
}

var rs1Letters = []rune("abcdefghijklmnopqrstuvwxyz")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = rs1Letters[rand.Intn(len(rs1Letters))]
	}
	return string(b)
}
