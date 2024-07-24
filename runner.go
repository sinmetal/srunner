package srunner

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/time/rate"
)

type Runnner interface {
	Run(ctx context.Context) error
}

type AppRunnner struct {
	parallelism int
	limiter     *rate.Limiter
}

func NewAppRunner(ctx context.Context, ratePerSec int, parallelism int) *AppRunnner {
	n := rate.Every(time.Second / time.Duration(ratePerSec))
	return &AppRunnner{
		parallelism: parallelism,
		limiter:     rate.NewLimiter(n, ratePerSec),
	}
}

// Run is 並行実行を行う
func (ar *AppRunnner) Run(ctx context.Context, funcName string, runnner Runnner) {
	for i := 0; i < ar.parallelism; i++ {
		go ar.internalRun(ctx, funcName, runnner)
	}
}

func (ar *AppRunnner) internalRun(ctx context.Context, funcName string, runnner Runnner) {
	var errorCount int
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("stop run %s\n", funcName)
			return
		default:
			if err := ar.limiter.Wait(ctx); err != nil {
				fmt.Printf("failed limitter funcName=%s, err=%s\n", funcName, err)
				time.Sleep(1 * time.Second)
				continue
			}
			if err := runnner.Run(ctx); err != nil {
				errorCount++
				fmt.Printf("failed %s. errCount=%d err=%s\n", funcName, errorCount, err)
				time.Sleep(time.Duration(600*errorCount+rand.Intn(600)) * time.Second)
				continue
			}
			errorCount = 0
		}
	}
}
