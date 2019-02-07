package main

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

func createClient(ctx context.Context, db string, o ...option.ClientOption) (*spanner.Client, error) {
	config := spanner.ClientConfig{}
	dataClient, err := spanner.NewClientWithConfig(ctx, db, config, o...)
	if err != nil {
		return nil, err
	}

	return dataClient, nil
}
