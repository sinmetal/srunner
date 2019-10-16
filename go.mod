module github.com/sinmetal/srunner

require (
	cloud.google.com/go v0.46.3
	contrib.go.opencensus.io/exporter/stackdriver v0.9.1
	git.apache.org/thrift.git v0.0.0-20181218151757-9b75e4fe745a // indirect
	github.com/census-instrumentation/opencensus-proto v0.1.0 // indirect
	github.com/gcpug/handy-spanner v0.0.0-20191014100656-870baecbd673 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/uuid v1.1.1
	github.com/grpc-ecosystem/grpc-gateway v1.6.2 // indirect
	github.com/kelseyhightower/envconfig v1.3.0
	github.com/openzipkin/zipkin-go v0.1.3 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.2 // indirect
	github.com/prometheus/common v0.0.0-20181218105931-67670fe90761 // indirect
	github.com/sinmetal/gcpmetadata v0.0.0-20190204122414-bb2afc737814
	go.opencensus.io v0.22.1
	golang.org/x/build v0.0.0-20190111050920-041ab4dc3f9d // indirect
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	google.golang.org/api v0.10.0
	google.golang.org/grpc v1.24.0
)

replace google.golang.org/grpc => github.com/kazegusuri/grpc-go v0.0.0-20190303174943-5a9bd920d6da
