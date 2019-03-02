package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/kelseyhightower/envconfig"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const Service = "srunner"

type EnvConfig struct {
	SpannerDatabase string `required:"true"`
	Goroutine       int    `default:"3"`
}

func main() {
	var env EnvConfig
	if err := envconfig.Process("srunner", &env); err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("ENV_CONFIG %+v\n", env)

	ctx := context.Background()
	sc, err := createClient(ctx, env.SpannerDatabase, option.WithGRPCDialOption(
		grpc.WithDialer(func(addr string, t time.Duration) (net.Conn, error) {
			defer func(n time.Time) {
				fmt.Printf("Dial time: %v\n", time.Since(n))
			}(time.Now())
			return net.DialTimeout("tcp", addr, t)
		}),
	),
		option.WithGRPCDialOption(
			grpc.WithTransportCredentials(&wrapTransportCredentials{
				TransportCredentials: credentials.NewClientTLSFromCert(nil, ""),
			}),
		))
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
