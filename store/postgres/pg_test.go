package postgres

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/breez/data-sync/store"
	"github.com/breez/data-sync/testutil"
	"github.com/stretchr/testify/require"
)

var testConnStr string

func TestMain(m *testing.M) {
	connStr, terminate, err := testutil.StartPostgres(context.Background())
	if err != nil {
		log.Fatalf("failed to start PostgreSQL container: %v", err)
	}
	defer terminate()
	testConnStr = connStr

	os.Exit(m.Run())
}

func TestAddRecords(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestAddRecords(t, storage)
}

func TestUpdateRecords(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestUpdateRecords(t, storage)
}

func TestConflict(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestConflict(t, storage)
}

func TestAcquireAndCheckLock(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestAcquireAndCheckLock(t, storage)
}

func TestLockExpiration(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestLockExpiration(t, storage)
}

func TestMultipleInstanceLocks(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestMultipleInstanceLocks(t, storage)
}

func TestReleaseLockIdempotent(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestReleaseLockIdempotent(t, storage)
}

func TestExclusiveLock(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestExclusiveLock(t, storage)
}

func TestExclusiveLockSameInstance(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestExclusiveLockSameInstance(t, storage)
}

func TestDeleteExpiredLocks(t *testing.T) {
	storage, err := NewPGSyncStorage(testConnStr)
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestDeleteExpiredLocks(t, storage)
}
