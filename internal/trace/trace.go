package trace

import (
	"context"
	"errors"
	"fmt"
	"log"

	"cloud.google.com/go/spanner"
	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	gcppropagator "github.com/GoogleCloudPlatform/opentelemetry-operations-go/propagator"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata/cloudrun"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
)

var tracer trace.Tracer
var meterProvider *sdkmetric.MeterProvider

func init() {
	ctx := context.Background()
	fmt.Println("trace init()")

	if metadatabox.OnCloudRun() {
		installPropagators()

		projectID, err := metadatabox.ProjectID()
		if err != nil {
			log.Fatalf("required google cloud project id: %v", err)
		}

		spanner.EnableOpenTelemetryMetrics()

		res, err := newResource(ctx, "", "")
		if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
			log.Println(err)
		} else if err != nil {
			log.Fatalf("resource.New: %v", err)
		}
		tp, err := getOtlpTracerProvider(ctx, projectID, res)
		if err != nil {
			log.Fatalf("getOtlpTracerProvider: %v", err)
		}
		// TODO Shutdownはどうやろう？ defer tp.Shutdown(ctx) // flushes any pending spans, and closes connections.
		otel.SetTracerProvider(tp)
		tracer = otel.GetTracerProvider().Tracer("github.com/sinmetal/srunner")

		// Create a new meter provider
		meterProvider, err = getOtlpMeterProvider(ctx, projectID, res)
		if err != nil {
			log.Fatalf("getOtlpMeterProvider: %v", err)
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

func newResource(ctx context.Context, serviceName string, revision string) (*resource.Resource, error) {
	return resource.New(ctx,
		// Use the GCP resource detector to detect information about the GCP platform
		resource.WithDetectors(gcp.NewDetector()),
		// Keep the default detectors
		resource.WithTelemetrySDK(),
		// Add your own custom attributes to identify your application
		resource.WithAttributes(
			semconv.ServiceNameKey.String("srunner"),
			semconv.ServiceVersion("0.0.0"),
		),
	)
}

func StartSpan(ctx context.Context, spanName string, ops ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, spanName, ops...)
}

func GetMeterProvider() *sdkmetric.MeterProvider {
	return meterProvider
}
