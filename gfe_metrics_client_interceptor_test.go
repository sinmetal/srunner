package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sapiv1 "cloud.google.com/go/spanner/apiv1"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Measure struct {
	ID         string
	CommitedAt time.Time
}

var gcpugPublicSpannerDB = fmt.Sprintf("projects/%s/instances/%s/databases/%s", "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal")

func TestGFEMetricsUnaryClientInterceptor(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened:           1,
			TrackSessionHandles: true,
		},
	}
	sc, err := spanner.NewClientWithConfig(ctx, gcpugPublicSpannerDB, config,
		option.WithGRPCDialOption(grpc.WithUnaryInterceptor(GFEMetricsUnaryClientInterceptor())),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	requestToSpanner(ctx, t, sc)
}

func TestGFEMetricsStreamClientInterceptor(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	config := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened:           1,
			TrackSessionHandles: true,
		},
	}
	sc, err := spanner.NewClientWithConfig(ctx, gcpugPublicSpannerDB, config,
		option.WithGRPCDialOption(grpc.WithStreamInterceptor(GFEMetricsStreamClientInterceptor())),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	requestToSpanner(ctx, t, sc)
}

func requestToSpanner(ctx context.Context, t *testing.T, sc *spanner.Client) {
	_, err := sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		keys := spanner.KeySetFromKeys(spanner.Key{"000008f7-b5a3-4ada-8852-f5bf63f9e8ef"})
		iter := tx.Read(ctx, "Measure", keys, []string{"ID"})
		defer iter.Stop()
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				t.Fatal(err)
			}
		}

		v := &Measure{
			ID:         uuid.New().String(),
			CommitedAt: spanner.CommitTimestamp,
		}
		mu, err := spanner.InsertStruct("Measure", v)
		if err != nil {
			return err
		}
		return tx.BufferWrite([]*spanner.Mutation{mu})
	})
	if err != nil {
		t.Fatal(err)
	}

	iter := sc.Single().Query(ctx, spanner.NewStatement("SELECT * FROM Measure LIMIT 10000"))
	defer iter.Stop()
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSpannerAPIV1(t *testing.T) {
	ctx := context.Background()

	sc, err := sapiv1.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	session, err := sc.CreateSession(ctx, &spannerpb.CreateSessionRequest{
		Database: gcpugPublicSpannerDB,
		Session:  nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := &spannerpb.ExecuteSqlRequest{
		Session:        session.GetName(),
		Transaction:    nil,
		Sql:            "SELECT 1",
		Params:         nil,
		ParamTypes:     nil,
		ResumeToken:    nil,
		QueryMode:      0,
		PartitionToken: nil,
		Seqno:          0,
		QueryOptions:   nil,
	}
	var md metadata.MD
	_, err = sc.ExecuteSql(ctx, req, gax.WithGRPCOptions(grpc.Header(&md)))
	if err != nil {
		t.Fatal(err)
	}
	srvTiming := md.Get("server-timing")
	t.Log(srvTiming)
}

func TestExtractServerTimingValue(t *testing.T) {
	cases := []struct {
		name string
		text string
		want int64
		ok   bool
	}{
		{"null", "", 0, false},
		{"exist", "gfet4t7; dur=2516", 2516, true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var md metadata.MD = make(map[string][]string)
			md.Set("server-timing", tt.text)
			got, ok := ExtractServerTimingValue(md)
			if ok != tt.ok {
				t.Errorf("want ok is %t but got %t,", tt.ok, ok)
			}
			if got != tt.want {
				t.Errorf("want %d but got %d", tt.want, got)
			}
		})
	}
}
