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
	stores1 "github.com/sinmetal/srunner/pattern1/stores"
	stores2 "github.com/sinmetal/srunner/pattern2/stores"
	"github.com/sinmetal/srunner/randdata"
	"github.com/sinmetal/srunner/tweet"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
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
	spannerDatabase2ID := os.Getenv("SRUNNER_SPANNER_DATABASE2_ID")

	dbName1 := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectID, spannerInstanceID, spannerDatabaseID)
	fmt.Println(dbName1)

	dbName2 := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectID, spannerInstanceID, spannerDatabase2ID)
	fmt.Println(dbName1)

	tokenSource, err := auth.DefaultTokenSourceWithProactiveCache(ctx)
	if err != nil {
		panic(err)
	}

	sc1, err := spanner.NewClient(ctx, dbName1, option.WithTokenSource(tokenSource))
	if err != nil {
		panic(err)
	}
	sc2, err := spanner.NewClient(ctx, dbName2, option.WithTokenSource(tokenSource))
	if err != nil {
		panic(err)
	}

	runTweet(ctx, sc1)

	// Pattern1, 2でランダムでデータ作ってたが、同じデータの方が分かりやすそう
	runSamplePattern1(ctx, sc1)
	runSamplePattern2(ctx, sc2)

	// Receive output from signalChan.
	sig := <-signalChan
	fmt.Printf("%s signal caught", sig)

	time.Sleep(10 * time.Second)
	tweetShutdownChan <- true
	samplePattern1ShutdownChan <- true

	sc1.Close()
	sc2.Close()
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
	ordersStore, err := stores1.NewOrdersStore(sc)
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
				var orderDetails []*stores1.OrderDetail
				detailCount := int(rand.Int63n(30)) + 1
				for i := 0; i < detailCount; i++ {
					item := randdata.GetItemPattern1()
					quantity := rand.Int63n(30) + 1
					v := &stores1.OrderDetail{
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

func runSamplePattern2(ctx context.Context, sc *spanner.Client) {
	usersStore, err := stores2.NewUsersStore(sc)
	if err != nil {
		panic(err)
	}

	ordersStore, err := stores2.NewOrdersStore(sc)
	if err != nil {
		panic(err)
	}

	for _, author := range randdata.GetAllAuthors() {
		_, err := usersStore.Insert(ctx, &stores2.User{
			UserID:   author,
			UserName: author,
		})
		if spanner.ErrCode(err) == codes.AlreadyExists {
			// noop
		} else if err != nil {
			panic(err)
		}
	}

	go func(ctx context.Context) {
		for {
			select {
			case <-samplePattern1ShutdownChan:
				fmt.Println("stop SamplePattern2 logic")
				return
			default:
				userID := randdata.GetAuthor()
				orderID := uuid.New().String()
				var orderDetails []*stores2.OrderDetail
				detailCount := int(rand.Int63n(30)) + 1
				for i := 0; i < detailCount; i++ {
					item := randdata.GetItemPattern1()
					quantity := rand.Int63n(30) + 1
					v := &stores2.OrderDetail{
						UserID:        userID,
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
					log.Printf("failed pattern2.OrdersStore.Insert() id=%s err=%s\n", orderID, err)
				}
			}
		}
	}(ctx)
}
