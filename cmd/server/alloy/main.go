package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sinmetal/srunner"
	"github.com/sinmetal/srunner/balance"
	"github.com/sinmetal/srunner/internal/alloy"
	"github.com/sinmetal/srunner/internal/profiler"
	"github.com/sinmetal/srunner/internal/trace"
)

var signalChan = make(chan os.Signal, 1)

const (
	serviceVersion = "v0.0.0"
)

// GRANT SELECT ON UserBalance TO "gke-worker-default@{PROJECT_ID}.iam";
// https://cloud.google.com/alloydb/docs/manage-iam-authn#gcloud_1
func main() {
	ctx := context.Background()

	fmt.Println("ignite")

	// SIGINT handles Ctrl+C locally.
	// SIGTERM handles Cloud Run termination signal.
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	user := os.Getenv("USER")
	fmt.Printf("user:%s\n", user)

	// "projects/{PROJECT_ID}/locations/asia-northeast1/clusters/playground/instances/playground-primary"
	instanceName := os.Getenv("INSTANCE_NAME")
	if instanceName == "" {
		panic("instance name is empty")
	}
	fmt.Printf("instance name:%s\n", instanceName)

	readReplicaInstanceName := os.Getenv("READ_REPLICA_INSTANCE_NAME")
	fmt.Printf("read replica instance name:%s\n", readReplicaInstanceName)

	password := os.Getenv("PASSWORD") // TODO passwordを適当になんとかする hello alloy
	if password == "" {
		panic("password is empty")
	}
	pgxCon, cleanup, err := alloy.ConnectPgx(ctx, instanceName, user, password, "quickstart_db")
	if err != nil {
		panic(err)
	}
	defer pgxCon.Close()
	defer func() {
		if err := cleanup(); err != nil {
			panic(fmt.Errorf("failed cleanup : %w", err))
		}
	}()
	func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := pgxCon.Ping(ctx); err != nil {
			panic(fmt.Errorf("failed ping : %w", err))
		}
	}()

	var readReplicaPgxPool []*pgxpool.Pool
	if len(readReplicaInstanceName) > 0 {
		pgxCon, cleanup, err := alloy.ConnectPgx(ctx, instanceName, user, password, "quickstart_db")
		if err != nil {
			panic(err)
		}
		defer pgxCon.Close()
		defer func() {
			if err := cleanup(); err != nil {
				panic(fmt.Errorf("failed cleanup : %w", err))
			}
		}()
		func() {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := pgxCon.Ping(ctx); err != nil {
				panic(fmt.Errorf("failed ping : %w", err))
			}
		}()
		readReplicaPgxPool = append(readReplicaPgxPool, pgxCon)
	}

	var serviceName = "srunner"
	configServiceName := os.Getenv("SRUNNER_SERVICE_NAME")
	if configServiceName != "" {
		serviceName = configServiceName
	}
	fmt.Printf("SRUNNER_SERVICE_NAME=%s\n", serviceName)

	trace.Init(ctx, serviceName, serviceVersion)
	if err := profiler.Init(ctx, serviceName, serviceVersion); err != nil {
		panic(err)
	}

	s := balance.NewStoreAlloy(pgxCon, readReplicaPgxPool)
	balanceRunner := &balance.DepositAlloyRunner{
		Store: s,
	}
	{
		ar := srunner.NewAppRunner(ctx, 50, 50)
		ar.Run(ctx, "Balance.Deposit", balanceRunner)
	}

	readUserBalanceRunner := &balance.ReadUserBalancesAlloyRunner{
		Store: s,
	}
	{
		ar := srunner.NewAppRunner(ctx, 50, 50)
		ar.Run(ctx, "Balance.ReadUserBalances", readUserBalanceRunner)
	}

	// Receive output from signalChan.
	sig := <-signalChan
	fmt.Printf("--%s signal caught--\n", sig)
	time.Sleep(10)
	fmt.Println("Shutdown srunner")
}

func CreateAlloyUser(ctx context.Context) {
	//for i := 1; i < 10000000; i++ {
	//	userID := balance.CreateUserID(ctx, int64(i))
	//	if err := s.InsertUserBalance(ctx, &balance.UserBalance{
	//		UserID: userID,
	//		Amount: 0,
	//		Point:  0,
	//	}); err != nil {
	//		panic(err)
	//	}
	//}
}
