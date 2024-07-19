package srunner

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

type Runnner struct {
}

// Run is 並行実行を行う
func (r *Runnner) Run(ctx context.Context, funcName string, parallelism int, f func(ctx context.Context) error) {
	for i := 0; i < parallelism; i++ {
		go r.internalRun(ctx, funcName, f)
	}
}

func (r *Runnner) internalRun(ctx context.Context, funcName string, f func(ctx context.Context) error) {
	var errorCount int
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("stop run %s\n", funcName)
			return
		default:
			if err := f(ctx); err != nil {
				errorCount++
				fmt.Printf("failed %s. errCount=%d err=%s\n", funcName, errorCount, err)
				time.Sleep(time.Duration(600*errorCount+rand.Intn(600)) * time.Second)
				continue
			}
			errorCount = 0
		}
	}
}
