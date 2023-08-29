package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/randdata"
	"github.com/sinmetal/srunner/tweet"
)

func main() {
	ctx := context.Background()

	spannerProjectID := os.Getenv("SRUNNER_SPANNER_PROJECT_ID")
	spannerInstanceID := os.Getenv("SRUNNER_SPANNER_INSTANCE_ID")
	spannerDatabaseID := os.Getenv("SRUNNER_SPANNER_DATABASE_ID")

	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectID, spannerInstanceID, spannerDatabaseID)
	fmt.Println(dbName)
	sc, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		panic(err)
	}

	ts := tweet.NewStore(sc)
	for {
		ctx := context.Background()
		id := uuid.New().String()
		now := time.Now()
		author := randdata.GetAuthor()
		favos := randdata.GetAuthors()
		_, err := ts.Insert(ctx, &tweet.Tweet{
			TweetID:       id,
			Author:        author,
			Content:       fmt.Sprintf("Hello. My name is %s. %s (%s*%s*%s)", author, now, uuid.New().String(), uuid.New().String(), uuid.New().String()),
			Favos:         favos,
			Sort:          rand.Int63(),
			CreatedAt:     now,
			UpdatedAt:     now,
			CommitedAt:    spanner.CommitTimestamp,
			SchemaVersion: 1,
		})
		if err != nil {
			fmt.Printf("failed TweetStore.Insert() id=%s err=%s\n", id, err)
		}
	}
}
