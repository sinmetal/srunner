package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kelseyhightower/envconfig"
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
