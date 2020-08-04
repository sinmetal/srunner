package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"cloud.google.com/go/spanner"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	metadatabox "github.com/sinmetal/gcpbox/metadata"
	"github.com/sinmetal/srunner/tweet"
	"github.com/sinmetal/stats"
	"go.opencensus.io/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const Service = "srunner"

type EnvConfig struct {
	SpannerDatabase string `required:"true"`
	Goroutine       int    `default:"3"`
	TracePrefix     string `default:"default"`
}

func main() {
	var env EnvConfig
	if err := envconfig.Process("srunner", &env); err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("ENV_CONFIG %+v\n", env)

	tracePrefix = env.TracePrefix

	project, err := metadatabox.ProjectID()
	if err != nil {
		panic(err)
	}
	zone, err := metadatabox.Zone()
	if err != nil {
		panic(err)
	}
	nodeID := uuid.New().String() // TODO 本当はPodのIDとかがいい

	{
		exporter, err := stackdriver.NewExporter(stackdriver.Options{
			ProjectID: project,
		})
		if err != nil {
			panic(err)
		}
		trace.RegisterExporter(exporter)
		// trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}
	{
		labels := &stackdriver.Labels{}
		labels.Set("Worker", nodeID, "Worker ID")
		var exporter = stats.InitExporter(project, zone, "srunner", nodeID, labels)
		if err := stats.InitOpenCensusStats(exporter); err != nil {
			panic(err)
		}
	}

	ctx := context.Background()

	// Need to specify scope for the specific service.
	tokenSource, err := DefaultTokenSourceWithProactiveCache(ctx, spanner.Scope)
	if err != nil {
		panic(err)
	}

	if err := spanner.EnableStatViews(); err != nil {
		panic(err)
	}
	sc, err := createClient(ctx, env.SpannerDatabase,
		option.WithGRPCDialOption(
			grpc.WithTransportCredentials(&wrapTransportCredentials{
				TransportCredentials: credentials.NewClientTLSFromCert(nil, ""),
			}),
		),
		option.WithTokenSource(tokenSource),
	)
	if err != nil {
		panic(err)
	}
	ts := tweet.NewTweetStore(sc)
	// ias := item.NewAllStore(ctx, sc)

	endCh := make(chan error, 10)

	goInsertTweet(ts, env.Goroutine, endCh)
	// goInsertTweetBenchmark(ts, env.Goroutine, endCh)
	// goInsertTweetWithFCFS(ts, env.Goroutine, endCh)
	goUpdateTweet(ts, env.Goroutine, endCh)
	goUpdateTweet(ts, rand.Intn(10)+env.Goroutine, endCh)
	// goUpdateTweetWithFCFS(ts, env.Goroutine, endCh)
	goGetExitsTweet(ts, env.Goroutine, endCh)
	goGetExitsTweet(ts, rand.Intn(10)+env.Goroutine, endCh)
	goQueryRandom(ts, env.Goroutine, endCh)
	//goGetExitsTweetFCFS(ts, env.Goroutine, endCh)
	//goGetNotFoundTweet(ts, env.Goroutine, endCh)
	//goGetNotFoundTweetFCFS(ts, env.Goroutine, endCh)
	// goGetTweet3Tables(ts, env.Goroutine, endCh)

	// goInsertItemOrder(ias, env.Goroutine, endCh)
	// goInsertItemOrderNOFK(ias, env.Goroutine, endCh)
	// goInsertItemOrderDummyFK(ias, env.Goroutine, endCh)

	err = <-endCh
	fmt.Printf("BOMB %+v", err)
	sc.Close()
}
