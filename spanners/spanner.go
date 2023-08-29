package spanners

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/log"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func CreateClient(ctx context.Context, db string, o ...option.ClientOption) (*spanner.Client, error) {
	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened:           400,
			MaxOpened:           600,
			TrackSessionHandles: true,
		},
	}
	dataClient, err := spanner.NewClientWithConfig(ctx, db, config, o...)
	if err != nil {
		return nil, err
	}

	return dataClient, nil
}

// Ready is 動作準備完了するまでブロックする
// Workload Identityは最初数秒間SAが来ない的な話があったと思ったので、それを待つためのもの
func Ready(ctx context.Context, sc *spanner.Client) {
	if !metadatabox.OnGCP() {
		// localでは即Return
		return
	}

	fmt.Println("Ready Start")
	start := time.Now()
	defer func() {
		fmt.Printf("Ready Finish. time:%s\n", start.Sub(time.Now()))
	}()

	sleepSec := 1
	for {
		saEmail, err := metadata.Email("")
		if err != nil {
			log.Fatal(ctx, err.Error())
		}
		log.Info(ctx, fmt.Sprintf("I am %s\n", saEmail))

		iter := sc.Single().Query(ctx, spanner.NewStatement("SELECT 1"))
		defer iter.Stop()
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				return
			} else if err != nil {
				fmt.Printf("try ready... next %d sec. %s\n", sleepSec, err)
				time.Sleep(time.Duration(sleepSec) * time.Second)
				break
			}
		}
		sleepSec *= 2
	}
}
