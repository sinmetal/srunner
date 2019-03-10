package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
	"google.golang.org/grpc/credentials"
)

func createClient(ctx context.Context, db string, o ...option.ClientOption) (*spanner.Client, error) {
	config := spanner.ClientConfig{
		NumChannels: 12,
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: 50,
		},
	}
	dataClient, err := spanner.NewClientWithConfig(ctx, db, config, o...)
	if err != nil {
		return nil, err
	}

	return dataClient, nil
}

type wrapTransportCredentials struct {
	credentials.TransportCredentials
}

func (c *wrapTransportCredentials) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	defer func(n time.Time) {
		fmt.Printf("ClientHandshake time: %v\n", time.Since(n))
	}(time.Now())
	return c.TransportCredentials.ClientHandshake(ctx, authority, rawConn)
}
