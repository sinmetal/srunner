package main

import (
	"context"

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
