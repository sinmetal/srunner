package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/auth"
	"github.com/sinmetal/srunner/randdata"
	"github.com/sinmetal/srunner/tweet"
	"google.golang.org/api/option"
)

var shutdownChan = make(chan bool)
var signalChan chan (os.Signal) = make(chan os.Signal, 1)

func main() {
	ctx := context.Background()

	fmt.Println("Ignition srunner")

	// SIGINT handles Ctrl+C locally.
	// SIGTERM handles Cloud Run termination signal.
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	spannerProjectID := os.Getenv("SRUNNER_SPANNER_PROJECT_ID")
	spannerInstanceID := os.Getenv("SRUNNER_SPANNER_INSTANCE_ID")
	spannerDatabaseID := os.Getenv("SRUNNER_SPANNER_DATABASE_ID")

	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectID, spannerInstanceID, spannerDatabaseID)
	fmt.Println(dbName)

	tokenSource, err := auth.DefaultTokenSourceWithProactiveCache(ctx)
	if err != nil {
		panic(err)
	}

	sc, err := spanner.NewClient(ctx, dbName, option.WithTokenSource(tokenSource))
	if err != nil {
		panic(err)
	}

	ts := tweet.NewStore(sc)

	go func(ctx context.Context) {
		for {
			select {
			case <-shutdownChan:
				fmt.Println("stop logic")
				sc.Close()
				return
			default:
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
					log.Printf("failed TweetStore.Insert() id=%s err=%s\n", id, err)
				}
			}
		}
	}(ctx)

	// Receive output from signalChan.
	sig := <-signalChan
	fmt.Printf("%s signal caught", sig)

	time.Sleep(10 * time.Second)
	shutdownChan <- true

	fmt.Println("Shutdown srunner")
}
