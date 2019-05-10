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

					ctx, span := startSpan(ctx, "/go/insertTweet")
					defer span.End()

					defer func(n time.Time) {
						fmt.Printf("GoRoutine:%d id:%s goInsertTweet_time: %v\n", i, id, time.Since(n))
					}(time.Now())

					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
						defer cancel()
					}

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
			sleep()
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
			sleep()
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

							defer func(n time.Time) {
								fmt.Printf("GoRoutine:%d id:%s goUpdateTweet_time: %v\n", i, id, time.Since(n))
							}(time.Now())

							var cancel context.CancelFunc
							if _, hasDeadline := ctx.Deadline(); !hasDeadline {
								ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
								defer cancel()
							}

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
			sleep()
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

							defer func(n time.Time) {
								fmt.Printf("GoRoutine:%d id:%s goGetExitsTweet_time: %v\n", i, id, time.Since(n))
							}(time.Now())

							var cancel context.CancelFunc
							if _, hasDeadline := ctx.Deadline(); !hasDeadline {
								ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
								defer cancel()
							}

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
			sleep()
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

					id := uuid.New().String()

					defer func(n time.Time) {
						fmt.Printf("GoRoutine:%d id:%s goGetNotFoundTweet_time: %v\n", i, id, time.Since(n))
					}(time.Now())

					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
						defer cancel()
					}

					key := spanner.Key{id}
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
			sleep()
		}
	}()
}

func goQueryHeavyTweet(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					ctx, span := startSpan(ctx, "/go/queryHeavyTweet")
					defer span.End()

					fmt.Printf("GoRoutine:%d Start goQueryHeavyTweet_time: 5000ms\n", i)
					defer func(n time.Time) {
						fmt.Printf("GoRoutine:%d goQueryHeavyTweet_time: %v\n", i, time.Since(n))
					}(time.Now())

					_, err := ts.QueryHeavy(ctx)
					if err != nil {
						ecode := spanner.ErrCode(err)
						if ecode != codes.NotFound {
							endCh <- err
						}
					}
				}(i)
			}
			wg.Wait()
			sleepLong()
		}
	}()
}

func goQueryAllTweet(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					ctx, span := startSpan(ctx, "/go/queryAllTweet")
					defer span.End()

					fmt.Printf("GoRoutine:%d Start goQueryAllTweet_time: 5000ms\n", i)
					defer func(n time.Time) {
						fmt.Printf("GoRoutine:%d goQueryAllTweet_time: %v\n", i, time.Since(n))
					}(time.Now())

					count, err := ts.QueryAll(ctx)
					if err != nil {
						ecode := spanner.ErrCode(err)
						if ecode != codes.NotFound {
							endCh <- err
						}
					}
					fmt.Printf("goQueryAllTweet: %d\n", count)
				}(i)
			}
			wg.Wait()
			sleepLong()
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

					defer func(n time.Time) {
						fmt.Printf("GoRoutine:%d goGetTweet3Tables_time: %v\n", i, time.Since(n))
					}(time.Now())

					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
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
			sleep()
		}
	}()
}

func sleep() {
	time.Sleep((time.Duration(300) + time.Duration(rand.Intn(300))) * time.Millisecond)
}

func sleepLong() {
	time.Sleep((time.Duration(100) + time.Duration(rand.Intn(30))) * time.Minute)
}
