package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/log"
	"github.com/sinmetal/srunner/score"
	"github.com/sinmetal/srunner/tweet"
	"golang.org/x/xerrors"
)

const (
	defaultTimeoutDuration = 3000 * time.Millisecond
)

var (
	exactStaleness30sec = spanner.MaxStaleness(30 * time.Second)
)

type RunnerV2 struct {
	ts             tweet.TweetStore
	scoreUserStore *score.ScoreUserStore
	scoreStore     *score.ScoreStore
	endCh          chan<- error
}

func (run *RunnerV2) outputMetrics(ctx context.Context, metricsID string, err error) {
	if ctx.Err() == context.DeadlineExceeded || xerrors.Is(err, context.DeadlineExceeded) {
		if err := CountSpannerStatus(context.Background(), metricsID, MetricsKindTimeout); err != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
		}
		return
	}
	if err != nil {
		log.Error(ctx, fmt.Sprintf("failed %s : %+v\n", metricsID, err))
		serr := CountSpannerStatus(context.Background(), metricsID, MetricsKindNG)
		if serr != nil {
			run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", serr)
		}
		return
	}

	if err := CountSpannerStatus(context.Background(), metricsID, MetricsKindOK); err != nil {
		run.endCh <- fmt.Errorf("failed stats.CountSpannerStatus : %w", err)
	}
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

	const metricsID = "INSERT TWEET"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
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
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
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

	const metricsID = "INSERT WITH OPERATION TWEET"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		retCh <- run.ts.InsertWithOperation(ctx, id)
	}()
	select {
	case <-ctx.Done():
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
	}
}

func (run *RunnerV2) GoUpdateTweet(concurrent int) {
	const limit = 1000
	idCh := make(chan string, 10000)
	go func() {
		t := time.NewTicker(1000 * time.Millisecond)
		defer t.Stop()

		last := &tweet.PageOptionForQueryOrderByCreatedAtDesc{
			ID:        "",
			CreatedAt: time.Now(),
		}
		for {
			select {
			case <-t.C:
				ctx := context.Background()

				tlist, err := run.ts.QueryOrderByCreatedAtDesc(ctx, 0, 3, last, limit)
				if err != nil {
					run.endCh <- fmt.Errorf("failed GoUpdateTweet : QueryResultStruct : %w", err)
				}
				for _, t := range tlist {
					idCh <- t.ID
				}

				if len(tlist) < limit {
					// 末尾 (最も古いデータまでいったら、先頭付近に戻る)
					last = &tweet.PageOptionForQueryOrderByCreatedAtDesc{
						ID:        "",
						CreatedAt: time.Now(),
					}
					continue
				}
				last = &tweet.PageOptionForQueryOrderByCreatedAtDesc{
					ID:        tlist[len(tlist)-1].ID,
					CreatedAt: tlist[len(tlist)-1].CreatedAt,
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

	const metricsID = "UPDATE TWEET"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		_, err := run.ts.Update(ctx, id)
		retCh <- err
	}()
	select {
	case <-ctx.Done():
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
	}
}

func (run *RunnerV2) GoUpdateDMLTweet(concurrent int) {
	const limit = 1000
	idCh := make(chan string, 10000)
	go func() {
		t := time.NewTicker(1000 * time.Millisecond)
		defer t.Stop()

		last := &tweet.PageOptionForQueryOrderByCreatedAtDesc{
			ID:        "",
			CreatedAt: time.Now(),
		}
		for {
			select {
			case <-t.C:
				ctx := context.Background()

				tlist, err := run.ts.QueryOrderByCreatedAtDesc(ctx, 5, 8, last, limit)
				if err != nil {
					run.endCh <- fmt.Errorf("failed GoUpdateTweet : QueryResultStruct : %w", err)
				}
				for _, t := range tlist {
					idCh <- t.ID
				}

				if len(tlist) < limit {
					// 末尾 (最も古いデータまでいったら、先頭付近に戻る)
					last = &tweet.PageOptionForQueryOrderByCreatedAtDesc{
						ID:        "",
						CreatedAt: time.Now(),
					}
					continue
				}
				last = &tweet.PageOptionForQueryOrderByCreatedAtDesc{
					ID:        tlist[len(tlist)-1].ID,
					CreatedAt: tlist[len(tlist)-1].CreatedAt,
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
					run.updateTweetDML(ctx, id)
				}
			}
		}()
	}
}

func (run *RunnerV2) updateTweetDML(ctx context.Context, id string) {
	ctx, span := startSpan(ctx, "goV2/updateTweetDML")
	defer span.End()

	const metricsID = "UPDATE DML TWEET"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		_, err := run.ts.UpdateDML(ctx, id)
		retCh <- err
	}()
	select {
	case <-ctx.Done():
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
	}
}

func (run *RunnerV2) GoDeleteTweet(concurrent int) {
	idCh := make(chan string, 10000)
	go func() {
		t := time.NewTicker(1000 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				ctx := context.Background()

				ids, err := run.ts.QueryResultStruct(ctx, true, 1000, &exactStaleness30sec)
				if err != nil {
					run.endCh <- fmt.Errorf("failed GoDeleteTweet : QueryResultStruct : %w", err)
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
					run.deleteTweet(ctx, id)
				}
			}
		}()
	}
}

func (run *RunnerV2) deleteTweet(ctx context.Context, id string) {
	ctx, span := startSpan(ctx, "goV2/deleteTweet")
	defer span.End()

	const metricsID = "DELETE TWEET"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		retCh <- run.ts.Delete(ctx, id)
	}()
	select {
	case <-ctx.Done():
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
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

				ids, err := run.ts.QueryResultStruct(ctx, false, 1000, &exactStaleness30sec)
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

	const metricsID = "GET TWEET"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		_, err := run.ts.Get(ctx, spanner.Key{id})
		retCh <- err
	}()
	select {
	case <-ctx.Done():
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
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
	ctx, span := startSpan(ctx, "goV2/queryTweetLatestByAuthor")
	defer span.End()

	const metricsID = "QUERY LATEST BY AUTHOR"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		_, err := run.ts.QueryLatestByAuthor(ctx, author, &exactStaleness30sec)
		retCh <- err
	}()
	select {
	case <-ctx.Done():
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
	}
}

func (run *RunnerV2) GoUpdateScore(concurrent int) {
	for i := 0; i < concurrent; i++ {
		go func() {
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					ctx := context.Background()
					run.updateScore(ctx)
				}
			}
		}()
	}
}

func (run *RunnerV2) updateScore(ctx context.Context) {
	ctx, span := startSpan(ctx, "goV2/updateScore")
	defer span.End()

	const metricsID = "UPDATE SCORE"

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeoutDuration)
		defer cancel()
	}

	retCh := make(chan error, 1)
	go func() {
		id := run.scoreUserStore.ID(ctx, rand.Int63n(1000000000))

		// circleにある程度偏りをもたらす
		var circleID int64
		turningPoint := rand.Intn(100)
		switch {
		case turningPoint > 90:
			{
				circleID = rand.Int63n(100)
			}
		case turningPoint > 70:
			{
				circleID = rand.Int63n(5000)
			}
		case turningPoint > 40:
			{
				circleID = rand.Int63n(10000)
			}
		default:
			circleID = rand.Int63n(100000)
		}

		// scoreにある程度偏りをもたらす
		var value int64
		turningPoint = rand.Intn(100)
		switch {
		case turningPoint > 95:
			{
				value = rand.Int63()
			}
		case turningPoint > 90:
			{
				value = rand.Int63n(5000000000)
			}
		case turningPoint > 85:
			{
				value = rand.Int63n(1000000000)
			}
		default:
			value = rand.Int63n(1000000)
		}
		err := run.scoreStore.Upsert(ctx, &score.Score{
			ID:       id,
			CircleID: run.scoreStore.CircleID(circleID),
			Score:    value,
		})
		retCh <- err
	}()
	select {
	case <-ctx.Done():
		run.outputMetrics(ctx, metricsID, nil)
	case err := <-retCh:
		run.outputMetrics(ctx, metricsID, err)
	}
}
