package tweet

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

func init() {
	rand.NewSource(time.Now().UnixNano())
}

func TestInParam(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal_benchmark_a")
	sc, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	idList := []string{"001d465a-8c67-4ba7-9f55-6b2fcf966a13", "003dad95-30f3-4449-8781-9025ab6ab80f"}

	statement := spanner.NewStatement("SELECT * FROM Item1K WHERE ItemId IN UNNEST (@IDList)")
	statement.Params["IDList"] = idList
	iter := sc.Single().Query(ctx, statement)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		var itemId string
		if err := row.ColumnByName("ItemId", &itemId); err != nil {
			t.Fatal(err)
		}
		t.Log("hit: ", itemId)
	}
}

func TestTweet_ToInsertOrUpdateMap(t *testing.T) {
	tweetID := "1195ab9a-4839-43ed-9ab4-e8ca3fecd481" // 固定のUUID
	now := time.Now()
	tweet := &Tweet{
		TweetID:       tweetID,
		Author:        "sinmetal",
		Content:       "Hello Test",
		Favos:         nil,
		Sort:          100,
		CreatedAt:     now,
		UpdatedAt:     now,
		SchemaVersion: 1,
	}
	in := tweet.ToInsertOrUpdateMap()
	{
		v, ok := in["TweetID"]
		if !ok {
			t.Errorf("TweetID not found")
		}
		if got, want := v, tweetID; got != want {
			t.Errorf("TweetID want %s but got %s", want, got)
		}
	}
	{
		v, ok := in["ShardID"]
		if !ok {
			t.Errorf("ShardID not found")
		}
		if got, want := v, int64(7); got != want {
			t.Errorf("ShardID want %d but got %d", want, got)
		}
	}

}

func TestDefaultTweetStore_Insert(t *testing.T) {
	ctx := context.Background()

	NewTestInstance(t)

	dbName := RandString(8)

	statements := readDDLFile(t, "../ddl/tweet.sql")
	NewDatabase(t, dbName, statements)

	sc, err := spanner.NewClient(ctx, fmt.Sprintf("projects/fake/instances/fake/databases/%s", dbName))
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	ts := NewStore(sc)

	now := time.Now()
	_, err = ts.Insert(ctx, &Tweet{
		TweetID:    "test",
		Author:     "sinmetal",
		Content:    "hello world",
		Favos:      []string{},
		Sort:       0,
		CreatedAt:  now,
		UpdatedAt:  now,
		CommitedAt: spanner.CommitTimestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefaultTweetStore_QueryOrderByCreatedAtDesc(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	NewTestInstance(t)

	dbName := RandString(8)

	statements := readDDLFile(t, "../ddl/tweet.sql")
	NewDatabase(t, dbName, statements)

	sc, err := spanner.NewClient(ctx, fmt.Sprintf("projects/fake/instances/fake/databases/%s", dbName))
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	ts := NewStore(sc)
	for i := 0; i < 100; i++ {
		now := time.Now()
		_, err = ts.Insert(ctx, &Tweet{
			TweetID:    fmt.Sprintf("%d", i),
			Author:     "sinmetal",
			Content:    "hello world",
			Favos:      []string{},
			Sort:       0,
			CreatedAt:  now,
			UpdatedAt:  now,
			CommitedAt: spanner.CommitTimestamp,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	tlist, err := ts.QueryOrderByCreatedAtDesc(ctx, 0, 9, &PageOptionForQueryOrderByCreatedAtDesc{
		ID:        "",
		CreatedAt: time.Now(),
	}, 10)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ts.QueryOrderByCreatedAtDesc(ctx, 0, 9, &PageOptionForQueryOrderByCreatedAtDesc{
		ID:        tlist[len(tlist)-1].TweetID,
		CreatedAt: tlist[len(tlist)-1].CreatedAt,
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].TweetID != "89" {
		t.Errorf("Desc で 11 番目だから、89 だと思ったのに！？ : %s", got[0].TweetID)
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
