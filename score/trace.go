package score

import (
	"context"
	"fmt"

	"go.opencensus.io/trace"
)

func startSpan(ctx context.Context, name string) (context.Context, *trace.Span) {
	return trace.StartSpan(ctx, fmt.Sprintf("/score/%s", name))
}
