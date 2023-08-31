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

	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", os.Args[1], os.Args[2], os.Args[3])
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
		fmt.Printf("INSERT %s %s(%s)\n", now, author, id)
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
			panic(err)
		}
	}
}
