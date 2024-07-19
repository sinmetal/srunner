package srunner

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

type Runnner interface {
	Run(ctx context.Context) error
}

type AppRunnner struct {
}

// Run is 並行実行を行う
func (ar *AppRunnner) Run(ctx context.Context, funcName string, parallelism int, runnner Runnner) {
	for i := 0; i < parallelism; i++ {
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
