package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StartPostgres spins up a disposable PostgreSQL container and returns its
// connection string. The container is terminated automatically when the
// test (or TestMain) finishes.
func StartPostgres(ctx context.Context) (connStr string, terminate func(), err error) {
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		return "", nil, err
	}

	connStr, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgContainer.Terminate(ctx)
		return "", nil, err
	}

	terminate = func() {
		pgContainer.Terminate(ctx)
	}

	return connStr, terminate, nil
}

// MustStartPostgres is like StartPostgres but calls t.Fatal on error
// and registers cleanup via t.Cleanup.
func MustStartPostgres(t *testing.T) string {
	t.Helper()
	connStr, terminate, err := StartPostgres(context.Background())
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}
	t.Cleanup(terminate)
	return connStr
}
