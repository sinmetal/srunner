package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sinmetal/srunner/balance"
	"github.com/sinmetal/srunner/internal/alloy"
)

// GRANT SELECT ON UserBalance TO "gke-worker-default@{PROJECT_ID}.iam";
// https://cloud.google.com/alloydb/docs/manage-iam-authn#gcloud_1
func main() {
	ctx := context.Background()

	fmt.Println("ignite")

	user := os.Getenv("USER")
	fmt.Printf("user:%s\n", user)

	// "projects/{PROJECT_ID}/locations/asia-northeast1/clusters/playground/instances/playground-primary"
	instanceName := os.Getenv("instanceName")
	if instanceName == "" {
		panic("instance name is empty")
	}
	password := os.Getenv("password") // TODO passwordを適当になんとかする helloalloy
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
			panic(err)
		}
	}()
	func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := pgxCon.Ping(ctx); err != nil {
			panic(err)
		}
	}()

	s := balance.NewStoreAlloy(pgxCon)
	fmt.Println("start insert UserBalance")
	if err := s.InsertUserBalance(ctx, &balance.UserBalance{
		UserID: "AlloyUser",
		Amount: 0,
		Point:  0,
	}); err != nil {
		panic(err)
	}
	fmt.Println("finish")
}
