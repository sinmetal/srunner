package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/profiler"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/kelseyhightower/envconfig"
	"github.com/sinmetal/gcpmetadata"
	"go.opencensus.io/trace"
)

const Service = "srunner"

type EnvConfig struct {
	SpannerDatabase string `required:"true"`
	Goroutine       int    `default:"10"`
}

func main() {
	var env EnvConfig
	if err := envconfig.Process("srunner", &env); err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("ENV_CONFIG %+v\n", env)

	project, err := gcpmetadata.GetProjectID()
	if err != nil {
		panic(err)
	}

	// Profiler initialization, best done as early as possible.
	if err := profiler.Start(profiler.Config{ProjectID: project, Service: Service, ServiceVersion: "0.0.1"}); err != nil {
		panic(err)
	}

	{
		exporter, err := stackdriver.NewExporter(stackdriver.Options{
			ProjectID:                project,
			TraceSpansBufferMaxBytes: 128 * 1024 * 1024, // defaultが8MBだが、めっちゃ増やしてみた
		})
		if err != nil {
			panic(err)
		}
		trace.RegisterExporter(exporter)
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}

	ctx := context.Background()
	sc, err := createClient(ctx, env.SpannerDatabase)
	if err != nil {
		panic(err)
	}
	ts := NewTweetStore(sc)

	endCh := make(chan error, 10)

	//goInsertTweet(ts, env.Goroutine, endCh)
	//goInsertTweetBenchmark(ts, env.Goroutine, endCh)
	//goUpdateTweet(ts, env.Goroutine, endCh)
	//goGetExitsTweet(ts, env.Goroutine, endCh)
	//goGetNotFoundTweet(ts, env.Goroutine, endCh)
	goGetTweet3Tables(ts, env.Goroutine, endCh)

	err = <-endCh
	fmt.Printf("BOMB %+v", err)
}
