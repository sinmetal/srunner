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
	rand.Seed(time.Now().UnixNano())
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

func TestDefaultTweetStore_QueryLatestByAuthor(t *testing.T) {
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

	ts := NewTweetStore(sc)
	now := time.Now()
	if err := ts.Insert(ctx, &Tweet{
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
	}); err != nil {
		t.Fatal(err)
	}

	_, err = ts.QueryLatestByAuthor(ctx, "sinmetal", nil)
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

	ts := NewTweetStore(sc)
	for i := 0; i < 100; i++ {
		now := time.Now()
		if err := ts.Insert(ctx, &Tweet{
			ID:             fmt.Sprintf("%d", i),
			Author:         "sinmetal",
			Content:        "hello world",
			Count:          0,
			Favos:          []string{},
			Sort:           0,
			ShardCreatedAt: 0,
			CreatedAt:      now,
			UpdatedAt:      now,
			CommitedAt:     spanner.CommitTimestamp,
		}); err != nil {
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
		ID:        tlist[len(tlist)-1].ID,
		CreatedAt: tlist[len(tlist)-1].CreatedAt,
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ID != "89" {
		t.Errorf("Desc で 11 番目だから、89 だと思ったのに！？ : %s", got[0].ID)
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
