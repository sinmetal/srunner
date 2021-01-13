package tweet

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	sadb "cloud.google.com/go/spanner/admin/database/apiv1"
	sai "cloud.google.com/go/spanner/admin/instance/apiv1"
	"google.golang.org/api/option"
	sadbpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	saipb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewTestInstance(t *testing.T) {
	ctx := context.Background()
	saiCli, err := sai.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ope, err := saiCli.CreateInstance(ctx, &saipb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/fake"),
		InstanceId: "fake",
	})
	if err != nil {
		sts, ok := status.FromError(err)
		if !ok {
			t.Fatal(err)
		}
		if sts.Code() == codes.AlreadyExists {
			// すでに存在するなら、それを使うので、スルー
			return
		}
		t.Fatal(err)
	}
	_, err = ope.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func NewDatabase(t *testing.T, databaseName string, statements []string) {
	ctx := context.Background()

	sadbCli, err := sadb.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ope, err := sadbCli.CreateDatabase(ctx, &sadbpb.CreateDatabaseRequest{
		Parent:          "projects/fake/instances/fake",
		CreateStatement: "CREATE DATABASE `" + databaseName + "`",
		ExtraStatements: statements,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ope.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func updateDDL(t *testing.T, dbName string, statements []string) {
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

	admin, err := sadb.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}

	_, err = admin.UpdateDatabaseDdl(ctx, &sadbpb.UpdateDatabaseDdlRequest{
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
