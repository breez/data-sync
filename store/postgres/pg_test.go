package postgres

import (
	"os"
	"testing"

	"github.com/breez/data-sync/store"
	"github.com/stretchr/testify/require"
)

func TestAddRecords(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestAddRecords(t, storage)
}

func TestUpdateRecords(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestUpdateRecords(t, storage)
}

func TestConflict(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestConflict(t, storage)
}

func TestAcquireAndCheckLock(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestAcquireAndCheckLock(t, storage)
}

func TestLockExpiration(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestLockExpiration(t, storage)
}

func TestMultipleInstanceLocks(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestMultipleInstanceLocks(t, storage)
}

func TestReleaseLockIdempotent(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestReleaseLockIdempotent(t, storage)
}

func TestDeleteExpiredLocks(t *testing.T) {
	storage, err := NewPGSyncStorage(os.Getenv("TEST_PG_DATABASE_URL"))
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestDeleteExpiredLocks(t, storage)
}
