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
	orders1 "github.com/sinmetal/srunner/pattern1/stores"
	"github.com/sinmetal/srunner/randdata"
	"github.com/sinmetal/srunner/tweet"
	"google.golang.org/api/option"
)

var tweetShutdownChan = make(chan bool)
var samplePattern1ShutdownChan = make(chan bool)
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

	runTweet(ctx, sc)
	runSamplePattern1(ctx, sc)

	// Receive output from signalChan.
	sig := <-signalChan
	fmt.Printf("%s signal caught", sig)

	time.Sleep(10 * time.Second)
	tweetShutdownChan <- true
	samplePattern1ShutdownChan <- true

	sc.Close()
	fmt.Println("Shutdown srunner")
}

func runTweet(ctx context.Context, sc *spanner.Client) {
	ts := tweet.NewStore(sc)

	go func(ctx context.Context) {
		for {
			select {
			case <-tweetShutdownChan:
				fmt.Println("stop tweet logic")
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
}

func runSamplePattern1(ctx context.Context, sc *spanner.Client) {
	ordersStore, err := orders1.NewOrdersStore(sc)
	if err != nil {
		panic(err)
	}

	go func(ctx context.Context) {
		for {
			select {
			case <-samplePattern1ShutdownChan:
				fmt.Println("stop SamplePattern1 logic")
				return
			default:
				userID := randdata.GetAuthor()
				orderID := uuid.New().String()
				var orderDetails []*orders1.OrderDetail
				detailCount := int(rand.Int63n(30)) + 1
				for i := 0; i < detailCount; i++ {
					item := randdata.GetItemPattern1()
					quantity := rand.Int63n(30) + 1
					v := &orders1.OrderDetail{
						OrderID:       orderID,
						OrderDetailID: int64(i + 1),
						ItemID:        item.ItemID,
						Price:         item.Price,
						Quantity:      quantity,
						CommitedAt:    time.Time{},
					}
					orderDetails = append(orderDetails, v)
				}
				_, err = ordersStore.Insert(ctx, userID, orderID, orderDetails)
				if err != nil {
					log.Printf("failed pattern1.OrdersStore.Insert() id=%s err=%s\n", orderID, err)
				}
			}
		}
	}(ctx)
}
