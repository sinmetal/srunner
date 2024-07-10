package trace

import (
	"context"
	"errors"
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	gcppropagator "github.com/GoogleCloudPlatform/opentelemetry-operations-go/propagator"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
)

var tracer trace.Tracer
var meterProvider *sdkmetric.MeterProvider

func Init(ctx context.Context, serviceName string, revision string) {
	fmt.Println("trace init()")

	// TODO Cloud Buildの時は動かないようにしているが、もうちょっといい方法が欲しいな
	if metadatabox.OnGCP() && os.Getenv("REF_NAME") == "" {
		_, err := setupOpenTelemetry(ctx)
		if err != nil {
			panic(err)
		}
	}
	if tracer == nil {
		fmt.Println("set default otel tracer")
		tracer = otel.Tracer("github.com/sinmetal/srunner")
	}
	if meterProvider == nil {
		// meterProvider = getOtlpMeterProvider(ctx, res)
		fmt.Println("not set meterProvider")
	}
	fmt.Printf("IsOpenTelemetryMetricsEnabled=%t\n", spanner.IsOpenTelemetryMetricsEnabled())
}

func setupOpenTelemetry(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown combines shutdown functions from multiple OpenTelemetry
	// components into a single function.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// Configure Context Propagation to use the default W3C traceparent format
	otel.SetTextMapPropagator(autoprop.NewTextMapPropagator())

	// Configure Trace Export to send spans as OTLP
	spanExporter, err := autoexport.NewSpanExporter(ctx)
	if err != nil {
		err = errors.Join(err, shutdown(ctx))
		return
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	shutdownFuncs = append(shutdownFuncs, tp.Shutdown)
	otel.SetTracerProvider(tp)

	// Configure Metric Export to send metrics as OTLP
	mreader, err := autoexport.NewMetricReader(ctx)
	if err != nil {
		err = errors.Join(err, shutdown(ctx))
		return
	}
	mp := metric.NewMeterProvider(
		metric.WithReader(mreader),
	)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)
	otel.SetMeterProvider(mp)

	return shutdown, nil
}

func installPropagators() {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			// Putting the CloudTraceOneWayPropagator first means the TraceContext propagator
			// takes precedence if both the traceparent and the XCTC headers exist.
			gcppropagator.CloudTraceOneWayPropagator{},
			propagation.TraceContext{},
			propagation.Baggage{},
		))
}

func getOtlpMeterProvider(ctx context.Context, projectID string, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	exporter, err := mexporter.New(
		mexporter.WithProjectID(projectID),
	)
	if err != nil {
		return nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	)
	return meterProvider, nil
}

func getOtlpTracerProvider(ctx context.Context, projectID string, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	exporter, err := texporter.New(
		texporter.WithProjectID(projectID),
		texporter.WithTraceClientOptions(
			[]option.ClientOption{option.WithTelemetryDisabled()}, // otelのtrace送信そのもののtraceは送らない
		),
	)
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // 1min間に1requestなので、全部出している
	)

	return tracerProvider, nil
}

func StartSpan(ctx context.Context, spanName string, ops ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, spanName, ops...)
}

func EndSpan(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
	span.End()
}

func GetMeterProvider() *sdkmetric.MeterProvider {
	return meterProvider
}
