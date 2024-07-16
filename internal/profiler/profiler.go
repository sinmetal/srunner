package profiler

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/profiler"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
)

func Init(ctx context.Context, service string, serviceVersion string) error {
	if !metadatabox.OnGCP() {
		return nil
	}

	projectID, err := metadatabox.ProjectID()
	if err != nil {
		log.Fatalf("required google cloud project id: %v", err)
	}
	cfg := profiler.Config{
		ProjectID:      projectID,
		Service:        service,
		ServiceVersion: serviceVersion,
		// ProjectID must be set if not running on GCP.
		// ProjectID: "my-project",

		// For OpenCensus users:
		// To see Profiler agent spans in APM backend,
		// set EnableOCTelemetry to true
		// EnableOCTelemetry: true,
		EnableOCTelemetry: true,
	}

	// Profiler initialization, best done as early as possible.
	if err := profiler.Start(cfg); err != nil {
		return fmt.Errorf("failed profiler.Init service:%s,serviceVersion=%s : %w", service, serviceVersion, err)
	}
	return nil
}
