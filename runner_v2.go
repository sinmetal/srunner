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
	"github.com/sinmetal/stats"
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
		if err := stats.CountSpannerStatus(context.Background(), "INSERT TIMEOUT"); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
	case err := <-retCh:
		if err != nil {
			serr := stats.CountSpannerStatus(context.Background(), "INSERT NG")
			if serr != nil {
				err = fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
			}
			if err != nil {
				run.endCh <- err
			}
		} else {
			if err := stats.CountSpannerStatus(context.Background(), "INSERT OK"); err != nil {
				run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
			}
		}
	}
}

func (run *RunnerV2) GoUpdateTweet(concurrent int) {
	idCh := make(chan string, 100000)
	go func() {
		t := time.NewTicker(1000 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				ctx := context.Background()

				ids, err := run.ts.QueryResultStruct(ctx, 5000)
				if err != nil {
					run.endCh <- err
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
		if err := stats.CountSpannerStatus(context.Background(), "UPDATE TIMEOUT"); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
	case err := <-retCh:
		if err != nil {
			serr := stats.CountSpannerStatus(context.Background(), "UPDATE NG")
			if serr != nil {
				err = fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
			}
			if err != nil {
				run.endCh <- err
			}
		} else {
			if err := stats.CountSpannerStatus(context.Background(), "UPDATE OK"); err != nil {
				run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
			}
		}
	}
}
