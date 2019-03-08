module github.com/sinmetal/srunner

require (
	cloud.google.com/go v0.36.0
	contrib.go.opencensus.io/exporter/stackdriver v0.9.1
	github.com/census-instrumentation/opencensus-proto v0.1.0 // indirect
	github.com/google/uuid v1.1.1
	github.com/kelseyhightower/envconfig v1.3.0
	github.com/pkg/errors v0.8.1
	github.com/sinmetal/gcpmetadata v0.0.0-20190204122414-bb2afc737814
	go.opencensus.io v0.19.0
	golang.org/x/oauth2 v0.0.0-20181203162652-d668ce993890
	google.golang.org/api v0.1.0
	google.golang.org/grpc v1.17.0
)

replace google.golang.org/grpc => github.com/kazegusuri/grpc-go v0.0.0-20190303174943-5a9bd920d6da
