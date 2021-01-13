package log

import (
	"context"
	"fmt"
	"log"
	"os"
)

// Info is info level log
// 暫定版
func Info(ctx context.Context, body string) {
	fmt.Println(body)
}

// Error is error level log
// 暫定版
func Error(ctx context.Context, body string) {
	log.Println(body)
}

func Fatal(ctx context.Context, body string) {
	log.Println(body)
	os.Exit(1)
}
