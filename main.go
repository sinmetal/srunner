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

const Service = "esrunner"

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
			ProjectID: project,
		})
		if err != nil {
			panic(err)
		}
		trace.RegisterExporter(exporter)
	}

	ctx := context.Background()
	sc, err := createClient(ctx, env.SpannerDatabase)
	if err != nil {
		panic(err)
	}
	ts := NewTweetStore(sc)

	endCh := make(chan error, 10)

	goInsertTweet(ts, env.Goroutine, endCh)
	//goInsertTweetBenchmark(ts, env.Goroutine, endCh)
	//goUpdateTweet(ts, env.Goroutine, endCh)
	//goGetExitsTweet(ts, env.Goroutine, endCh)
	//goGetNotFoundTweet(ts, env.Goroutine, endCh)

	err = <-endCh
	fmt.Printf("BOMB %+v", err)
}
