package alloy

import (
	"context"
	"fmt"
	"net"

	"cloud.google.com/go/alloydbconn"
	"github.com/jackc/pgx/v5/pgxpool"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
)

// connectPgx establishes a connection to your database using pgxpool and the
// AlloyDB Go Connector (aka alloydbconn.Dialer)
//
// The function takes an instance URI, a username, a password, and a database
// name. Usage looks like this:
//
//	pool, cleanup, err := connectPgx(
//	  context.Background(),
//	  "projects/myproject/locations/us-central1/clusters/mycluster/instances/myinstance",
//	  "postgres",
//	  "secretpassword",
//	  "mydb",
//	)
//
// In addition to a *pgxpool.Pool type, the function returns a cleanup function
// that should be called when you're done with the database connection.
func ConnectPgx(
	ctx context.Context, instURI, user, pass, dbname string,
) (*pgxpool.Pool, func() error, error) {
	// First initialize the dialer. alloydbconn.NewDialer accepts additional
	// options to configure credentials, timeouts, etc.
	//
	// For details, see:
	// https://pkg.go.dev/cloud.google.com/go/alloydbconn#Option
	d, err := alloydbconn.NewDialer(ctx, alloydbconn.WithIAMAuthN())
	if err != nil {
		noop := func() error { return nil }
		return nil, noop, fmt.Errorf("failed to init Dialer: %v", err)
	}
	// The cleanup function will stop the dialer's background refresh
	// goroutines. Call it when you're done with your database connection to
	// avoid a goroutine leak.
	cleanup := func() error { return d.Close() }

	dsn := fmt.Sprintf(
		// sslmode is disabled, because the Dialer will handle the SSL
		// connection instead.
		"user=%s password=%s dbname=%s sslmode=disable",
		user, pass, dbname,
	)

	// Prefer pgxpool for applications.
	// For more information, see:
	// https://github.com/jackc/pgx/wiki/Getting-started-with-pgx#using-a-connection-pool
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, cleanup, fmt.Errorf("failed to parse pgx config: %v", err)
	}

	// Tell pgx to use alloydbconn.Dialer to connect to the instance.
	config.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		if !metadatabox.OnGCP() {
			return d.Dial(ctx, instURI, alloydbconn.WithPublicIP())
		}
		return d.Dial(ctx, instURI)
	}

	// Establish the connection.
	pool, connErr := pgxpool.NewWithConfig(ctx, config)
	if connErr != nil {
		return nil, cleanup, fmt.Errorf("failed to connect: %s", connErr)
	}

	return pool, cleanup, nil
}
