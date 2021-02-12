package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// GFEMetricsUnaryClientInterceptor is server-timing をログ出力する
// https://medium.com/google-cloud/use-gfe-server-timing-header-in-cloud-spanner-debugging-d7d891a50642
func GFEMetricsUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		fmt.Printf("%s:%s\n", "GFEMetricsUnaryClientInterceptor", method)
		var md metadata.MD
		addOpts := append(opts, grpc.Header(&md))

		defer func() {
			v, ok := ExtractServerTimingValue(md)
			if !ok {
				return
			}
			span := trace.FromContext(ctx)
			span.AddAttributes(trace.Int64Attribute("server-timing", v))
		}()

		return invoker(ctx, method, req, reply, cc, addOpts...)
	}
}

// GFEMetricsStreamClientInterceptor is server-timing をログ出力する
// https://medium.com/google-cloud/use-gfe-server-timing-header-in-cloud-spanner-debugging-d7d891a50642
func GFEMetricsStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		fmt.Printf("%s:%s\n", "GFEMetricsStreamClientInterceptor", method)
		var md metadata.MD
		addOpts := append(opts, grpc.Header(&md))

		defer func() {
			// TODO metadata 取れるけど、中身空っぽだなー
			v, ok := ExtractServerTimingValue(md)
			if !ok {
				return
			}
			span := trace.FromContext(ctx)
			span.AddAttributes(trace.Int64Attribute("server-timing", v))
		}()

		return streamer(ctx, desc, cc, method, addOpts...)
	}
}

func ExtractServerTimingValue(md metadata.MD) (int64, bool) {
	metaValues := md.Get("server-timing")
	for _, mv := range metaValues {
		v := strings.ReplaceAll(mv, "gfet4t7; dur=", "")
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	}
	return 0, false
}
