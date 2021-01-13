package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/sinmetal/srunner/log"
	"github.com/sinmetal/srunner/tweet"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
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
	ctx := context.Background()

	var env EnvConfig
	if err := envconfig.Process("srunner", &env); err != nil {
		log.Fatal(ctx, err.Error())
	}
	log.Info(ctx, fmt.Sprintf("ENV_CONFIG %+v\n", env))

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
		labels.Set("Spanner", env.SpannerDatabase, "Target Spanner Database")
		var exporter = InitExporter(project, zone, "srunner", nodeID, labels)
		if err := InitOpenCensusStats(exporter); err != nil {
			panic(err)
		}
	}

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

	runnerV2 := &RunnerV2{
		ts:    ts,
		endCh: endCh,
	}

	// 秒間 50 Requestにするための concurrent count
	// 200 ms ごとに実行されるので、default は秒間 5, なので、concurrent は 10 になる
	const concurrentReq50PerSec = 10
	const concurrentReq5PerSec = 1

	runnerV2.GoInsertTweet(concurrentReq5PerSec)
	runnerV2.GoInsertTweetWithOperation(concurrentReq5PerSec)
	runnerV2.GoUpdateTweet(concurrentReq5PerSec)
	runnerV2.GoDeleteTweet(concurrentReq50PerSec)
	runnerV2.GoGetTweet(concurrentReq5PerSec)
	runnerV2.GoQueryTweetLatestByAuthor(1) // 秒間 5回ほど, Author の種類が少ないので、同時実行無しで控えめ

	//goInsertTweet(ts, env.Goroutine, endCh)
	// goInsertTweetBenchmark(ts, env.Goroutine, endCh)
	// goInsertTweetWithFCFS(ts, env.Goroutine, endCh)
	//goUpdateTweet(ts, env.Goroutine, endCh)
	//goUpdateTweet(ts, rand.Intn(10)+env.Goroutine, endCh)
	// goUpdateTweetWithFCFS(ts, env.Goroutine, endCh)
	//goGetExitsTweet(ts, env.Goroutine, endCh)
	//goGetExitsTweet(ts, rand.Intn(10)+env.Goroutine, endCh)

	// Query Stats 水増し Random Query
	// goQueryRandom(ts, env.Goroutine, endCh)

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
