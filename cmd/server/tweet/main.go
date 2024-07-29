package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner"
	"github.com/sinmetal/srunner/balance"
	"github.com/sinmetal/srunner/internal/profiler"
	"github.com/sinmetal/srunner/internal/trace"
	"github.com/sinmetal/srunner/randdata"
	"github.com/sinmetal/srunner/tweet"
)

var signalChan = make(chan os.Signal, 1)

const (
	serviceVersion = "v0.0.0"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("Ignition srunner")

	// SIGINT handles Ctrl+C locally.
	// SIGTERM handles Cloud Run termination signal.
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	spannerProjectID := os.Getenv("SRUNNER_SPANNER_PROJECT_ID")
	spannerInstanceID := os.Getenv("SRUNNER_SPANNER_INSTANCE_ID")
	spannerDatabaseID := os.Getenv("SRUNNER_SPANNER_DATABASE_ID")

	dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectID, spannerInstanceID, spannerDatabaseID)
	fmt.Println(dbName)

	var serviceName = "srunner"
	configServiceName := os.Getenv("SRUNNER_SERVICE_NAME")
	if configServiceName != "" {
		serviceName = configServiceName
	}
	fmt.Printf("SRUNNER_SERVICE_NAME=%s\n", serviceName)

	runner, err := runner()
	if err != nil {
		panic(err)
	}

	trace.Init(ctx, serviceName, serviceVersion)
	if err := profiler.Init(ctx, serviceName, serviceVersion); err != nil {
		panic(err)
	}

	// meterProvider := trace.GetMeterProvider() // otel.SetMeterProviderでglobalにセットしている
	sc, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		panic(err)
	}

	balanceStore, err := balance.NewStore(ctx, sc)
	if err != nil {
		panic(err)
	}

	balanceDepositRunner := &balance.DepositRunner{
		BalanceStore: balanceStore,
	}
	balanceDepositDMLRunner := &balance.DepositDMLRunner{
		BalanceStore: balanceStore,
	}

	if _, ok := runner["CREATE_USER_ACCOUNT"]; ok {
		fmt.Println("Ignite CREATE_USER_ACCOUNT")
		if err := runCreateUserAccount(ctx, balanceStore, 1, balance.UserAccountIDMax); err != nil {
			panic(err)
		}
	}
	if rate, ok := runner["DEPOSIT"]; ok {
		fmt.Printf("Ignite DEPOSIT:%d\n", rate)
		ar := srunner.NewAppRunner(ctx, rate, 200)
		ar.Run(ctx, "Balance.Deposit", balanceDepositRunner)
	}
	if rate, ok := runner["DEPOSIT_DML"]; ok {
		fmt.Printf("Ignite DEPOSIT_DML:%d\n", rate)
		ar := srunner.NewAppRunner(ctx, rate, 200)
		ar.Run(ctx, "Balance.DepositDML", balanceDepositDMLRunner)
	}
	if _, ok := runner["TWEET"]; ok {
		fmt.Println("Ignite TWEET")
		ts := tweet.NewStore(sc)
		go runTweet(ctx, ts)
	}

	// Receive output from signalChan.
	sig := <-signalChan
	fmt.Printf("--%s signal caught--\n", sig)
	cancel()
	time.Sleep(10)
	sc.Close()
	fmt.Println("Shutdown srunner")
}

func runner() (map[string]int, error) {
	runner := make(map[string]int)
	runnersParam := os.Getenv("SRUNNER_RUNNERS") // DEPOSIT:10;TWEET:1 というformatを期待している
	runners := strings.Split(runnersParam, ";")
	for _, v := range runners {
		l := strings.Split(v, ":")
		if len(l) > 1 {
			parallels, err := strconv.Atoi(l[1])
			if err != nil {
				return nil, fmt.Errorf("invalid $SRUNNER_RUNNERS format %s : %w", runnersParam, err)
			}
			runner[l[0]] = parallels
		} else if len(l) == 1 {
			runner[l[0]] = 1
		}
	}
	return runner, nil
}

func runTweet(ctx context.Context, ts tweet.Store) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("stop run tweet")
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
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond) // Insert頻度を少し抑えつつ、ランダム要素を加える
		}
	}
}

func runCreateUserAccount(ctx context.Context, bs *balance.Store, idRangeStart, idRangeEnd int64) error {
	fmt.Println("start runCreateUserAccount")

	for i := idRangeStart; i <= idRangeEnd; i++ {
		userID := balance.CreateUserID(ctx, i)
		_, err := bs.CreateUserAccount(ctx, &balance.UserAccount{
			UserID: userID,
			Age:    int64(rand.Intn(100)),
			Height: int64(50 + rand.Intn(150)),
			Weight: int64(30 + rand.Intn(100)),
		})
		if err != nil {
			return fmt.Errorf("failed balance.CreateUserAccount idRange=%d-%d; userID=%s : %w", idRangeStart, idRangeEnd, userID, err)
		}
	}
	return nil
}
