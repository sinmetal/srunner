package main

import (
	"context"
	"fmt"

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
			fmt.Printf("server-timing:%+v\n", md.Get("server-timing"))
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
			fmt.Printf("server-timing:%+v\n", md.Get("server-timing"))
		}()

		return streamer(ctx, desc, cc, method, addOpts...)
	}
}
