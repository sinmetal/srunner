package main

import (
	"context"
	"fmt"

	"go.opencensus.io/trace"
)

var tracePrefix string

func startSpan(ctx context.Context, name string) (context.Context, *trace.Span) {
	return trace.StartSpan(ctx, fmt.Sprintf("/srunner/%s/%s", tracePrefix, name))
}
