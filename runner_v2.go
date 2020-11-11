package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/tweet"
)

type RunnerV2 struct {
	ts    tweet.TweetStore
	endCh chan<- error
}

func (run *RunnerV2) GoInsertTweet(concurrent int) {
	for i := 0; i < concurrent; i++ {
		go func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					ctx := context.Background()

					id := uuid.New().String()
					run.insertTweet(ctx, id)
				}
			}
		}()
	}
}

func (run *RunnerV2) insertTweet(ctx context.Context, id string) {
	ctx, span := startSpan(ctx, "goV2/insertTweet")
	defer span.End()

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
	}

	now := time.Now()
	shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10

	retCh := make(chan error, 1)
	go func() {
		retCh <- run.ts.Insert(ctx, &tweet.Tweet{
			ID:             id,
			Author:         getAuthor(),
			Content:        uuid.New().String(),
			Favos:          getAuthors(),
			Sort:           rand.Int63n(100000000),
			ShardCreatedAt: int64(shardId),
			CreatedAt:      now,
			UpdatedAt:      now,
			CommitedAt:     spanner.CommitTimestamp,
		})
	}()
	select {
	case <-ctx.Done():
		if err := CountSpannerStatus(context.Background(), "INSERT TIMEOUT", MetricsKindTimeout); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
	case err := <-retCh:
		if err != nil {
			serr := CountSpannerStatus(context.Background(), "INSERT NG", MetricsKindTimeout)
			if serr != nil {
				err = fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
			}
			fmt.Printf("failed InsertTweet : %+v\n", err)
		} else {
			if err := CountSpannerStatus(context.Background(), "INSERT OK", MetricsKindTimeout); err != nil {
				run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
			}
		}
	}
}

func (run *RunnerV2) GoInsertTweetWithOperation(concurrent int) {
	for i := 0; i < concurrent; i++ {
		go func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					ctx := context.Background()

					id := uuid.New().String()
					run.insertTweetWithOperation(ctx, id)
				}
			}
		}()
	}
}

func (run *RunnerV2) insertTweetWithOperation(ctx context.Context, id string) {
	ctx, span := startSpan(ctx, "goV2/insertTweetWithOperation")
	defer span.End()

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		retCh <- run.ts.InsertWithOperation(ctx, id)
	}()
	select {
	case <-ctx.Done():
		if err := CountSpannerStatus(context.Background(), "INSERT WITH Operation TIMEOUT", MetricsKindTimeout); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
	case err := <-retCh:
		if err != nil {
			serr := CountSpannerStatus(context.Background(), "INSERT WITH Operation NG", MetricsKindTimeout)
			if serr != nil {
				err = fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
			}
			fmt.Printf("failed InsertTweet : %+v\n", err)
		} else {
			if err := CountSpannerStatus(context.Background(), "INSERT WITH Operation OK", MetricsKindTimeout); err != nil {
				run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
			}
		}
	}
}

func (run *RunnerV2) GoUpdateTweet(concurrent int) {
	idCh := make(chan string, 10000)
	go func() {
		t := time.NewTicker(1000 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				ctx := context.Background()

				ids, err := run.ts.QueryResultStruct(ctx, 1000)
				if err != nil {
					run.endCh <- fmt.Errorf("failed GoUpdateTweet : QueryResultStruct : %w", err)
				}
				for _, id := range ids {
					idCh <- id.ID
				}
			}
		}
	}()

	for i := 0; i < concurrent; i++ {
		go func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					ctx := context.Background()

					id := <-idCh
					run.updateTweet(ctx, id)
				}
			}
		}()
	}
}

func (run *RunnerV2) updateTweet(ctx context.Context, id string) {
	ctx, span := startSpan(ctx, "goV2/updateTweet")
	defer span.End()

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		retCh <- run.ts.Update(ctx, id)
	}()
	select {
	case <-ctx.Done():
		if err := CountSpannerStatus(context.Background(), "UPDATE TIMEOUT", MetricsKindTimeout); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
	case err := <-retCh:
		if err != nil {
			serr := CountSpannerStatus(context.Background(), "UPDATE NG", MetricsKindTimeout)
			if serr != nil {
				err = fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
			}
			fmt.Printf("failed UpdateTweet : %+v\n", err)
		} else {
			if err := CountSpannerStatus(context.Background(), "UPDATE OK", MetricsKindTimeout); err != nil {
				run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
			}
		}
	}
}

func (run *RunnerV2) GoGetTweet(concurrent int) {
	idCh := make(chan string, 10000)
	go func() {
		t := time.NewTicker(1000 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				ctx := context.Background()

				ids, err := run.ts.QueryResultStruct(ctx, 1000)
				if err != nil {
					run.endCh <- fmt.Errorf("failed GoGetTweet : QueryResultStruct : %w", err)
				}
				for _, id := range ids {
					idCh <- id.ID
				}
			}
		}
	}()

	for i := 0; i < concurrent; i++ {
		go func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					ctx := context.Background()

					id := <-idCh
					run.getTweet(ctx, id)
				}
			}
		}()
	}
}

func (run *RunnerV2) getTweet(ctx context.Context, id string) {
	ctx, span := startSpan(ctx, "goV2/getTweet")
	defer span.End()

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		_, err := run.ts.Get(ctx, spanner.Key{id})
		retCh <- err
	}()
	select {
	case <-ctx.Done():
		if err := CountSpannerStatus(context.Background(), "GET TWEET TIMEOUT", MetricsKindTimeout); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
	case err := <-retCh:
		if err != nil {
			serr := CountSpannerStatus(context.Background(), "GET TWEET NG", MetricsKindNG)
			if serr != nil {
				err = fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
			}
			fmt.Printf("failed GetTweet : %+v\n", err)
		} else {
			if err := CountSpannerStatus(context.Background(), "GET TWEET OK", MetricsKindOK); err != nil {
				run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
			}
		}
	}
}

func (run *RunnerV2) GoQueryTweetLatestByAuthor(concurrent int) {
	for i := 0; i < concurrent; i++ {
		go func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					ctx := context.Background()

					author := getAuthor()
					run.queryTweetLatestByAuthor(ctx, author)
				}
			}
		}()
	}
}

func (run *RunnerV2) queryTweetLatestByAuthor(ctx context.Context, author string) {
	ctx, span := startSpan(ctx, "goV2/getTweetLatestByAuthor")
	defer span.End()

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		_, err := run.ts.QueryLatestByAuthor(ctx, author)
		retCh <- err
	}()
	select {
	case <-ctx.Done():
		if err := CountSpannerStatus(context.Background(), "QUERY LATEST BY AUTHOR TIMEOUT", MetricsKindTimeout); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
	case err := <-retCh:
		if err != nil {
			serr := CountSpannerStatus(context.Background(), "QUERY LATEST BY AUTHOR NG", MetricsKindNG)
			if serr != nil {
				err = fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
			}
			fmt.Printf("failed QueryLatestByAuthor : %+v\n", err)
		} else {
			if err := CountSpannerStatus(context.Background(), "QUERY LATEST BY AUTHOR OK", MetricsKindOK); err != nil {
				run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
			}
		}
	}
}
