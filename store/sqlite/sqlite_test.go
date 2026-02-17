package sqlite

import (
	"testing"

	"github.com/breez/data-sync/store"
	"github.com/stretchr/testify/require"
)

func TestAddRecords(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testaddrecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestAddRecords(t, storage)
}

func TestUpdateRecords(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testupdaterecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestUpdateRecords(t, storage)
}

func TestConflict(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testconflicts?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestConflict(t, storage)
}

func TestAcquireAndCheckLock(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testacquirelock?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestAcquireAndCheckLock(t, storage)
}

func TestLockExpiration(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testlockexpiration?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestLockExpiration(t, storage)
}

func TestMultipleInstanceLocks(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testmultiinstancelocks?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestMultipleInstanceLocks(t, storage)
}

func TestReleaseLockIdempotent(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testreleaselockidempotent?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestReleaseLockIdempotent(t, storage)
}

func TestExclusiveLock(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testexclusivelock?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestExclusiveLock(t, storage)
}

func TestExclusiveLockSameInstance(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testexclusivelocksameinstance?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestExclusiveLockSameInstance(t, storage)
}

func TestDeleteExpiredLocks(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testdeleteexpiredlocks?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	(&store.StoreTest{}).TestDeleteExpiredLocks(t, storage)
}
