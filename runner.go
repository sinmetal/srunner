package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"

	_ "google.golang.org/grpc/grpclog/glogger"
)

func goInsertTweet(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					id := uuid.New().String()
					now := time.Now()
					shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10

					if err := ts.Insert(ctx, &Tweet{
						ID:             id,
						Author:         getAuthor(),
						Content:        uuid.New().String(),
						Favos:          getAuthors(),
						Sort:           rand.Int63n(100000000),
						ShardCreatedAt: int64(shardId),
						CreatedAt:      now,
						UpdatedAt:      now,
						CommitedAt:     spanner.CommitTimestamp,
					}); err != nil {
						endCh <- err
					}
					fmt.Printf("TWEET_INSERT ID = %s, i = %d\n", id, i)
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goInsertTweetBenchmark(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					id := uuid.New().String()
					if err := ts.InsertBench(ctx, id); err != nil {
						endCh <- err
					}
					fmt.Printf("TWEET_INSERT_BENCH ID = %s, i = %d\n", id, i)
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goUpdateTweet(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					ids, err := ts.QueryResultStruct(ctx)
					if err != nil {
						endCh <- err
					}
					for _, id := range ids {
						id := id
						f := func(id string) {
							ctx, span := startSpan(ctx, "/go/updateTweet")
							defer span.End()
							if err := ts.Update(ctx, id); err != nil {
								ecode := spanner.ErrCode(err)
								if ecode == codes.NotFound {
									fmt.Printf("TWEET NOTFOUND ID = %s, i = %d\n", id, i)
									return
								}
								endCh <- err
							}
							fmt.Printf("TWEET_UPDATE ID = %s, i = %d\n", id, i)
						}
						f(id.ID)
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goGetExitsTweet(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()

					ids, err := ts.QueryResultStruct(ctx)
					if err != nil {
						endCh <- err
					}
					for _, id := range ids {
						id := id
						f := func(id string) {
							ctx, span := startSpan(ctx, "/go/getExitsTweet")
							defer span.End()

							key := spanner.Key{id}
							_, err := ts.Get(ctx, key)
							if err != nil {
								ecode := spanner.ErrCode(err)
								if ecode == codes.NotFound {
									fmt.Printf("TWEET %s is NOT FOUND !?", id)
									return
								}
								endCh <- err
							}
						}
						f(id.ID)
					}

				}(i)
			}
			wg.Wait()
		}
	}()
}

func goGetNotFoundTweet(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					ctx, span := startSpan(ctx, "/go/getNotFoundTweet")
					defer span.End()

					key := spanner.Key{uuid.New().String()}
					_, err := ts.Get(ctx, key)
					if err != nil {
						ecode := spanner.ErrCode(err)
						if ecode != codes.NotFound {
							endCh <- err
						}
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goGetTweet3Tables(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				i := i
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					ctx := context.Background()
					ctx, span := startSpan(ctx, "/go/goGetTweet3Tables")
					defer span.End()

					fmt.Printf("%+v goGetTweet3Tables GoRoutine:%d\n", time.Now(), i)
					defer func(n time.Time) {
						fmt.Printf("goGetTweet3Tables_time: %v\n", time.Since(n))
					}(time.Now())

					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
						defer cancel()
					}
					key := spanner.Key{uuid.New().String()}
					_, err := ts.GetTweet3Tables(ctx, key)
					if err != nil {
						ecode := spanner.ErrCode(err) // NOTFOUNDの時はGetTweet3Tablesがerr=nilで返してくるので、実際にはここは意味ない
						if ecode != codes.NotFound {
							endCh <- err
						}
					}
				}(i)
			}
			wg.Wait()
			time.Sleep((time.Duration(100) + time.Duration(rand.Intn(300))) * time.Millisecond)
		}
	}()
}
