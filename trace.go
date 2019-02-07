package main

import (
	"context"
	"fmt"

	"go.opencensus.io/trace"
)

func startSpan(ctx context.Context, name string) (context.Context, *trace.Span) {
	return trace.StartSpan(ctx, fmt.Sprintf("/srunner/%s", name), trace.WithSampler(trace.AlwaysSample()))
}
