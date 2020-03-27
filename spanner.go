package main

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
	"google.golang.org/grpc/credentials"
)

func createClient(ctx context.Context, db string, o ...option.ClientOption) (*spanner.Client, error) {
	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MaxIdle:       2000,
			MaxOpened:     2000,
			MinOpened:     1000,
			WriteSessions: 0.0,
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
