package spannertest

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	sadb "cloud.google.com/go/spanner/admin/database/apiv1"
	sai "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	sadbpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	saipb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewInstance(projectID string, instanceID string) error {
	ctx := context.Background()

	if len(os.Getenv("SPANNER_EMULATOR_HOST")) < 1 {
		return fmt.Errorf("SPANNER_EMULATOR_HOST is required")
	}

	saiCli, err := sai.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	ope, err := saiCli.CreateInstance(ctx, &saipb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
	})
	if err != nil {
		sts, ok := status.FromError(err)
		if !ok {
			return err
		}
		if sts.Code() == codes.AlreadyExists {
			// すでに存在するなら、それを使うので、スルー
			return nil
		}
		return err
	}
	_, err = ope.Wait(ctx)
	if err != nil {
		return err
	}
	return nil
}

func NewDatabase(t *testing.T, projectID string, instanceID string, databaseName string, statements []string) string {
	t.Helper()

	ctx := context.Background()

	IsSpannerEmulatorHost(t)

	sadbCli, err := sadb.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ope, err := sadbCli.CreateDatabase(ctx, &sadbpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", databaseName),
		ExtraStatements: statements,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ope.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseName)
}

func RandomDatabaseName() string {
	v := uuid.NewString()
	v = strings.ReplaceAll(v, "-", "")
	v = v[:28]
	return "a" + strings.ToLower(v)
}

func updateDDL(t *testing.T, dbName string, statements []string) {
	ctx := context.Background()

	IsSpannerEmulatorHost(t)

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

func ReadDDLFile(t *testing.T, path string) []string {
	t.Helper()

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

func IsSpannerEmulatorHost(t *testing.T) {
	t.Helper()

	if len(os.Getenv("SPANNER_EMULATOR_HOST")) < 1 {
		t.Fatalf("SPANNER_EMULATOR_HOST is required")
	}
}
