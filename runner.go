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
	"github.com/morikuni/failure"
	"github.com/sinmetal/srunner/item"
	"github.com/sinmetal/srunner/tweet"
	"github.com/sinmetal/stats"
	"github.com/tenntenn/sync/fcfs"
	"google.golang.org/grpc/codes"
)

var Timeout failure.StringCode = "TIMEOUT"
var NotFound failure.StringCode = "NOT_FOUND"

func goInsertTweet(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					id := uuid.New().String()

					ctx, span := startSpan(ctx, "go/insertTweet")
					defer span.End()

					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, 800*time.Millisecond)
						defer cancel()
					}

					now := time.Now()
					shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10

					retCh := make(chan error, 1)
					go func() {
						retCh <- ts.Insert(ctx, &tweet.Tweet{
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
						fmt.Printf("TWEET_INSERT_TIMEOUT ID = %s, i = %d\n", id, i)
						if err := stats.CountSpannerStatus(context.Background(), "INSERT TIMEOUT"); err != nil {
							endCh <- err
						}
					case err := <-retCh:
						if err != nil {
							serr := stats.CountSpannerStatus(context.Background(), "INSERT NG")
							if serr != nil {
								err = failure.Wrap(err, failure.Messagef("failed stats. err=%+v", serr))
							}
							if err != nil {
								endCh <- err
							}
						} else {
							if err := stats.CountSpannerStatus(context.Background(), "INSERT OK"); err != nil {
								endCh <- err
							}
						}
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goInsertTweetWithFCFS(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					id := uuid.New().String()

					ctx, span := startSpan(ctx, "go/insertTweetWithFCFS")
					defer span.End()

					now := time.Now()
					shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10

					t := &tweet.Tweet{
						ID:             id,
						Author:         getAuthor(),
						Content:        uuid.New().String(),
						Favos:          getAuthors(),
						Sort:           rand.Int63n(100000000),
						ShardCreatedAt: int64(shardId),
						CreatedAt:      now,
						UpdatedAt:      now,
						CommitedAt:     spanner.CommitTimestamp,
					}

					timeout := 1000 * time.Millisecond
					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, timeout)
						defer cancel()
					}

					var g fcfs.Group
					g.Go(func() (interface{}, error) {
						ctx, span := startSpan(ctx, "insert1")
						defer span.End()
						return "", ts.Insert(ctx, t)
					})

					g.Delay(250*time.Millisecond, func() (interface{}, error) {
						select {
						case <-ctx.Done():
							return "", nil
						default:
							ctx, span := startSpan(ctx, "insert2")
							defer span.End()
							err := stats.CountSpannerStatus(context.Background(), "INSERT_FCFS_SECOND START")
							if err != nil {
								return "", err
							}
							return "", ts.Insert(ctx, t)
						}
					})

					g.Go(func() (interface{}, error) {
						<-ctx.Done()
						return "", failure.New(Timeout)
					})

					_, err := g.Wait()
					if failure.Is(err, Timeout) {
						fmt.Printf("TWEET_INSERT_TIMEOUT ID = %s, i = %d\n", id, i)
						if err := stats.CountSpannerStatus(context.Background(), "INSERT_FCFS TIMEOUT"); err != nil {
							endCh <- err
						}
						return
					} else if err != nil {
						fmt.Printf("TWEET_INSERT_NG ID = %s, i = %d\n", id, i)
						serr := stats.CountSpannerStatus(context.Background(), "INSERT_FCFS NG")
						if serr != nil {
							err = failure.Wrap(err, failure.Messagef("failed stats. err=%+v", serr))
						}
						if err != nil {
							endCh <- err
						}
						return
					}
					if err := stats.CountSpannerStatus(context.Background(), "INSERT_FCFS OK"); err != nil {
						endCh <- err
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goFCFS(ctx context.Context, operation string, id string, i int, f func(ctx context.Context, id string, i int) (interface{}, error)) error {
	var g fcfs.Group
	g.Go(func() (interface{}, error) {
		ctx, span := startSpan(ctx, "ope1")
		defer span.End()
		return f(ctx, id, i)
	})

	g.Delay(250*time.Millisecond, func() (interface{}, error) {
		select {
		case <-ctx.Done():
			return "", nil
		default:
			ctx, span := startSpan(ctx, "ope2")
			defer span.End()
			err := stats.CountSpannerStatus(context.Background(), fmt.Sprintf("%s_FCFS_SECOND START", operation))
			if err != nil {
				return "", err
			}
			return f(ctx, id, i)
		}
	})

	g.Go(func() (interface{}, error) {
		<-ctx.Done()
		return "", failure.New(Timeout)
	})

	_, err := g.Wait()
	code, _ := failure.CodeOf(err)
	if code == Timeout {
		fmt.Printf("TWEET_%s_TIMEOUT ID = %s, i = %d\n", operation, id, i)
		if err := stats.CountSpannerStatus(context.Background(), fmt.Sprintf("%s_FCFS TIMEOUT", operation)); err != nil {
			return err
		}
	} else if err != nil {
		ecode := spanner.ErrCode(err)
		if ecode == codes.NotFound {
			return failure.New(NotFound)
		}

		fmt.Printf("TWEET_%s_NG ID = %s, i = %d\n", operation, id, i)
		serr := stats.CountSpannerStatus(context.Background(), fmt.Sprintf("%s_FCFS NG", operation))
		if serr != nil {
			err = failure.Wrap(err, failure.Messagef("failed stats. err=%+v", serr))
		}
		if err != nil {
			return err
		}
	} else {
		if err := stats.CountSpannerStatus(context.Background(), fmt.Sprintf("%s_FCFS OK", operation)); err != nil {
			return err
		}
	}

	return nil
}

func goInsertTweetBenchmark(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					ctx := context.Background()
					ctx, span := startSpan(ctx, "go/insertTweetBenchmark")
					defer span.End()

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

func goUpdateTweet(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			ids, err := ts.QueryResultStruct(ctx, goroutine)
			if err != nil {
				endCh <- err
			}
			if len(ids) < goroutine {
				// 最初はTableが空っぽでUPDATE対象のRowが取れないので、INSERTが走るまで少し待つ
				time.Sleep(5 * time.Minute)
				continue
			}

			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				i := i
				go func(i int) {
					defer wg.Done()

					f := func(id string) {
						ctx, span := startSpan(ctx, "go/updateTweet")
						defer span.End()

						var cancel context.CancelFunc
						if _, hasDeadline := ctx.Deadline(); !hasDeadline {
							ctx, cancel = context.WithTimeout(ctx, 800*time.Millisecond)
							defer cancel()
						}

						retCh := make(chan error, 1)
						go func() {
							retCh <- ts.Update(ctx, id)
						}()
						select {
						case <-ctx.Done():
							if err := stats.CountSpannerStatus(context.Background(), "UPDATE TIMEOUT"); err != nil {
								endCh <- err
							}
						case err := <-retCh:
							if err != nil {
								ecode := spanner.ErrCode(err)
								if ecode == codes.NotFound {
									fmt.Printf("TWEET NOTFOUND ID = %s, i = %d\n", id, i)
									return
								}

								serr := stats.CountSpannerStatus(context.Background(), "UPDATE NG")
								if serr != nil {
									err = failure.Wrap(err, failure.Messagef("failed stats. err=%+v", serr))
								}
								if err != nil {
									endCh <- err
								}
							} else {
								if err := stats.CountSpannerStatus(context.Background(), "UPDATE OK"); err != nil {
									endCh <- err
								}
							}
						}
					}

					id := ids[i]
					f(id.ID)
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goUpdateTweetWithFCFS(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			ids, err := ts.QueryResultStruct(ctx, goroutine)
			if err != nil {
				endCh <- err
			}
			if len(ids) < goroutine {
				// 最初はTableが空っぽでUPDATE対象のRowが取れないので、INSERTが走るまで少し待つ
				time.Sleep(5 * time.Minute)
				continue
			}

			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				i := i
				go func(i int) {
					defer wg.Done()

					f := func(id string) {
						ctx, span := startSpan(ctx, "go/updateTweetWithFCFS")
						defer span.End()

						var cancel context.CancelFunc
						if _, hasDeadline := ctx.Deadline(); !hasDeadline {
							ctx, cancel = context.WithTimeout(ctx, 1000*time.Millisecond)
							defer cancel()
						}

						err := goFCFS(ctx, "UPDATE", id, i, func(ctx context.Context, id string, i int) (interface{}, error) {
							return "", ts.Update(ctx, id)
						})
						if failure.Is(err, NotFound) {
							fmt.Printf("TWEET_UPDATE TARGET %s IS NOT_FOUND\n", id)
						} else if err != nil {
							endCh <- err
						}
					}

					id := ids[i]
					f(id.ID)
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goGetExitsTweet(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()

					ids, err := ts.QueryResultStruct(ctx, 10)
					if err != nil {
						endCh <- err
					}
					for _, id := range ids {
						id := id
						f := func(id string) {
							ctx, span := startSpan(ctx, "go/getExitsTweet")
							defer span.End()

							var cancel context.CancelFunc
							if _, hasDeadline := ctx.Deadline(); !hasDeadline {
								ctx, cancel = context.WithTimeout(ctx, 800*time.Millisecond)
								defer cancel()
							}

							retCh := make(chan error, 1)
							go func(id string) {
								key := spanner.Key{id}
								_, err := ts.Get(ctx, key)
								retCh <- err
							}(id)
							select {
							case <-ctx.Done():
								if err := stats.CountSpannerStatus(context.Background(), "GET_EXISTS TIMEOUT"); err != nil {
									endCh <- err
								}
							case err := <-retCh:
								if err != nil {
									if err != nil {
										ecode := spanner.ErrCode(err)
										if ecode == codes.NotFound {
											fmt.Printf("TWEET %s is NOT FOUND !?", id)
											return
										}
									}

									serr := stats.CountSpannerStatus(context.Background(), "GET_EXISTS NG")
									if serr != nil {
										err = failure.Wrap(err, failure.Messagef("failed stats. err=%+v", serr))
									}
									if err != nil {
										endCh <- err
									}
								} else {
									if err := stats.CountSpannerStatus(context.Background(), "GET_EXISTS OK"); err != nil {
										endCh <- err
									}
								}
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

func goGetExitsTweetFCFS(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()

					ids, err := ts.QueryResultStruct(ctx, 10)
					if err != nil {
						endCh <- err
					}
					for _, id := range ids {
						id := id
						f := func(id string) {
							ctx, span := startSpan(ctx, "go/getExitsTweetWithFCFS")
							defer span.End()

							var cancel context.CancelFunc
							if _, hasDeadline := ctx.Deadline(); !hasDeadline {
								ctx, cancel = context.WithTimeout(ctx, 800*time.Millisecond)
								defer cancel()
							}

							key := spanner.Key{id}
							var g fcfs.Group
							g.Go(func() (interface{}, error) {
								return ts.Get(ctx, key)
							})

							g.Delay(150*time.Millisecond, func() (interface{}, error) {
								return ts.Get(ctx, key)
							})

							g.Go(func() (interface{}, error) {
								<-ctx.Done()
								return "", failure.New(Timeout)
							})

							_, err := g.Wait()
							if failure.Is(err, Timeout) {
								if err := stats.CountSpannerStatus(context.Background(), "GET_EXISTS_FCFS TIMEOUT"); err != nil {
									endCh <- err
								}
							} else if err != nil {
								ecode := spanner.ErrCode(err)
								if ecode == codes.NotFound {
									fmt.Printf("TWEET %s is NOT FOUND !?", id)
									return
								}
								serr := stats.CountSpannerStatus(context.Background(), "GET_EXISTS_FCFS NG")
								if serr != nil {
									err = failure.Wrap(err, failure.Messagef("failed stats. err=%+v", serr))
								}
								if err != nil {
									endCh <- err
								}
							} else {
								if err := stats.CountSpannerStatus(context.Background(), "GET_EXISTS_FCFS OK"); err != nil {
									endCh <- err
								}
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

func goGetNotFoundTweet(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					ctx, span := startSpan(ctx, "go/getNotFoundTweet")
					defer span.End()

					id := uuid.New().String()

					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, 800*time.Millisecond)
						defer cancel()
					}

					retCh := make(chan error, 1)
					go func(id string) {
						key := spanner.Key{id}
						_, err := ts.Get(ctx, key)
						retCh <- err
					}(id)
					select {
					case <-ctx.Done():
						if err := stats.CountSpannerStatus(context.Background(), "GET_NOT_FOUND TIMEOUT"); err != nil {
							endCh <- err
						}
					case err := <-retCh:
						if err != nil {
							ecode := spanner.ErrCode(err)
							if ecode == codes.NotFound {
								if err := stats.CountSpannerStatus(context.Background(), "GET_NOT_FOUND OK"); err != nil {
									endCh <- err
								}
								return
							}

							serr := stats.CountSpannerStatus(context.Background(), "GET_NOT_FOUND NG")
							if serr != nil {
								err = failure.Wrap(err, failure.Messagef("failed stats. err=%+v", serr))
							}
							if err != nil {
								endCh <- err
							}
						}
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goGetNotFoundTweetFCFS(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx := context.Background()
					ctx, span := startSpan(ctx, "go/getNotFoundTweetWithFCFS")
					defer span.End()

					id := uuid.New().String()

					var cancel context.CancelFunc
					if _, hasDeadline := ctx.Deadline(); !hasDeadline {
						ctx, cancel = context.WithTimeout(ctx, 800*time.Millisecond)
						defer cancel()
					}

					key := spanner.Key{id}
					var g fcfs.Group
					g.Go(func() (interface{}, error) {
						return ts.Get(ctx, key)
					})

					g.Delay(150*time.Millisecond, func() (interface{}, error) {
						return ts.Get(ctx, key)
					})

					g.Go(func() (interface{}, error) {
						<-ctx.Done()
						return "", failure.New(Timeout)
					})

					_, err := g.Wait()
					if failure.Is(err, Timeout) {
						if err := stats.CountSpannerStatus(context.Background(), "GET_NOT_FOUND_FCFS TIMEOUT"); err != nil {
							endCh <- err
						}
					} else if err != nil {
						ecode := spanner.ErrCode(err)
						if ecode == codes.NotFound {
							if err := stats.CountSpannerStatus(context.Background(), "GET_NOT_FOUND_FCFS OK"); err != nil {
								endCh <- err
							}
						}
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goGetTweet3Tables(ts tweet.TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				i := i
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					ctx := context.Background()
					ctx, span := startSpan(ctx, "go/goGetTweet3Tables")
					defer span.End()

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
		}
	}()
}

func goInsertItemOrder(as *item.AllStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					ctx := context.Background()
					ctx, span := startSpan(ctx, "go/insertItemOrder")
					defer span.End()

					id := uuid.New().String()
					if err := as.IOS.Insert(ctx, &item.ItemOrder{
						ItemOrderID: id,
						ItemID:      as.IMS.GetRandomID(),
						UserID:      as.US.GetRandomID(),
						CommitedAt:  spanner.CommitTimestamp,
					}); err != nil {
						endCh <- err
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}

func goInsertItemOrderNOFK(as *item.AllStore, goroutine int, endCh chan<- error) {
	go func() {
		for {
			var wg sync.WaitGroup
			for i := 0; i < goroutine; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					ctx := context.Background()
					ctx, span := startSpan(ctx, "go/insertItemOrderNOFK")
					defer span.End()

					id := uuid.New().String()
					if err := as.IOSFK.Insert(ctx, &item.ItemOrderNOFK{
						ItemOrderID: id,
						ItemID:      as.IMS.GetRandomID(),
						UserID:      as.US.GetRandomID(),
						CommitedAt:  spanner.CommitTimestamp,
					}); err != nil {
						endCh <- err
					}
				}(i)
			}
			wg.Wait()
		}
	}()
}
