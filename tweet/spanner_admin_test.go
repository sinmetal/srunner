package tweet

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

func ddl(t *testing.T, dbName string, statements []string) {
	ctx := context.Background()

	var opts []option.ClientOption
	if emulatorAddr := os.Getenv("SPANNER_EMULATOR_HOST"); emulatorAddr != "" {
		emulatorOpts := []option.ClientOption{
			option.WithEndpoint(emulatorAddr),
			option.WithGRPCDialOption(grpc.WithInsecure()),
			option.WithoutAuthentication(),
		}
		opts = append(opts, emulatorOpts...)
	}

	admin, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}

	_, err = admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbName,
		Statements: statements,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func readDDLFile(t *testing.T, path string) []string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	l := strings.Split(string(data), ";")

	var ret []string
	for _, v := range l {
		nv := strings.Replace(v, "\n", " ", -1)
		nv = strings.TrimSpace(nv)
		nv = strings.Trim(nv, ";")
		if len(nv) < 10 {
			continue
		}
		ret = append(ret, nv)
	}
	return ret
}
