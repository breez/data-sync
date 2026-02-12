package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type StoreTest struct{}

func (s *StoreTest) TestAddRecords(t *testing.T, storage SyncStorage) {

	testStoreID := uuid.New().String()
	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0, "0.0.1")
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, uint64(1))

	newRevision, err = storage.SetRecord(context.Background(), testStoreID, "a2", []byte("data2"), 0, "0.0.1")
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, uint64(2))

	records, err := storage.ListChanges(context.Background(), testStoreID, 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{Id: "a1", Data: []byte("data1"), Revision: 1},
		{Id: "a2", Data: []byte("data2"), Revision: 2},
	})

	// Test different store with same id
	anotherStoreID := uuid.New().String()
	newRev, err := storage.SetRecord(context.Background(), anotherStoreID, "a1", []byte("data1"), 0, "0.0.1")
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRev, uint64(1))
}

func (s *StoreTest) TestUpdateRecords(t *testing.T, storage SyncStorage) {
	testStoreID := uuid.New().String()
	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0, "0.0.1")
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, uint64(1))

	newRevision, err = storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data2"), 1, "0.0.1")
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, uint64(2))

	records, err := storage.ListChanges(context.Background(), testStoreID, 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{Id: "a1", Data: []byte("data2"), Revision: 2},
	})
}

func (s *StoreTest) TestConflict(t *testing.T, storage SyncStorage) {
	testStoreID := uuid.New().String()
	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0, "0.0.1")
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, uint64(1))

	_, err = storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data2"), 0, "0.0.1")
	require.Error(t, err, "should have return with error")
	require.Equal(t, err, ErrSetConflict)
}

func (s *StoreTest) TestAcquireAndCheckLock(t *testing.T, storage SyncStorage) {
	userID := uuid.New().String()
	lockName := "test_lock"
	instanceID := uuid.New().String()

	// Initially no lock
	locked, err := storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.False(t, locked)

	// Acquire lock
	err = storage.SetLock(context.Background(), userID, lockName, instanceID, true, 30, false)
	require.NoError(t, err)

	// Should be locked
	locked, err = storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.True(t, locked)

	// Release lock
	err = storage.SetLock(context.Background(), userID, lockName, instanceID, false, 0, false)
	require.NoError(t, err)

	// Should be unlocked
	locked, err = storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.False(t, locked)
}

func (s *StoreTest) TestLockExpiration(t *testing.T, storage SyncStorage) {
	userID := uuid.New().String()
	lockName := "test_lock"
	instanceID := uuid.New().String()

	// Acquire lock with 1 second TTL
	err := storage.SetLock(context.Background(), userID, lockName, instanceID, true, 1, false)
	require.NoError(t, err)

	// Should be locked immediately
	locked, err := storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.True(t, locked)

	// Wait for expiry
	time.Sleep(2 * time.Second)

	// Should be unlocked after TTL
	locked, err = storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.False(t, locked)
}

func (s *StoreTest) TestMultipleInstanceLocks(t *testing.T, storage SyncStorage) {
	userID := uuid.New().String()
	lockName := "test_lock"
	instanceA := uuid.New().String()
	instanceB := uuid.New().String()

	// Both instances acquire
	err := storage.SetLock(context.Background(), userID, lockName, instanceA, true, 30, false)
	require.NoError(t, err)
	err = storage.SetLock(context.Background(), userID, lockName, instanceB, true, 30, false)
	require.NoError(t, err)

	// Should be locked
	locked, err := storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.True(t, locked)

	// Release instance A
	err = storage.SetLock(context.Background(), userID, lockName, instanceA, false, 0, false)
	require.NoError(t, err)

	// Still locked (instance B holds it)
	locked, err = storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.True(t, locked)

	// Release instance B
	err = storage.SetLock(context.Background(), userID, lockName, instanceB, false, 0, false)
	require.NoError(t, err)

	// Now unlocked
	locked, err = storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.False(t, locked)
}

func (s *StoreTest) TestReleaseLockIdempotent(t *testing.T, storage SyncStorage) {
	userID := uuid.New().String()
	lockName := "test_lock"
	instanceID := uuid.New().String()

	// Release a lock that was never acquired — should not error
	err := storage.SetLock(context.Background(), userID, lockName, instanceID, false, 0, false)
	require.NoError(t, err)
}

func (s *StoreTest) TestExclusiveLock(t *testing.T, storage SyncStorage) {
	userID := uuid.New().String()
	lockName := "test_lock"
	instanceA := uuid.New().String()
	instanceB := uuid.New().String()

	// Instance A acquires non-exclusive lock
	err := storage.SetLock(context.Background(), userID, lockName, instanceA, true, 30, false)
	require.NoError(t, err)

	// Instance B tries exclusive acquire — should fail
	err = storage.SetLock(context.Background(), userID, lockName, instanceB, true, 30, true)
	require.ErrorIs(t, err, ErrLockHeld)

	// Release instance A
	err = storage.SetLock(context.Background(), userID, lockName, instanceA, false, 0, false)
	require.NoError(t, err)

	// Instance B tries exclusive acquire again — should succeed
	err = storage.SetLock(context.Background(), userID, lockName, instanceB, true, 30, true)
	require.NoError(t, err)

	// Instance A tries non-exclusive acquire while B holds exclusive — succeeds (non-exclusive always succeeds)
	err = storage.SetLock(context.Background(), userID, lockName, instanceA, true, 30, false)
	require.NoError(t, err)

	// Release both
	err = storage.SetLock(context.Background(), userID, lockName, instanceA, false, 0, false)
	require.NoError(t, err)
	err = storage.SetLock(context.Background(), userID, lockName, instanceB, false, 0, false)
	require.NoError(t, err)
}

func (s *StoreTest) TestExclusiveLockSameInstance(t *testing.T, storage SyncStorage) {
	userID := uuid.New().String()
	lockName := "test_lock"
	instanceA := uuid.New().String()

	// Instance A acquires non-exclusive lock
	err := storage.SetLock(context.Background(), userID, lockName, instanceA, true, 30, false)
	require.NoError(t, err)

	// Same instance tries exclusive acquire — should succeed (only checks OTHER instances)
	err = storage.SetLock(context.Background(), userID, lockName, instanceA, true, 30, true)
	require.NoError(t, err)

	// Release
	err = storage.SetLock(context.Background(), userID, lockName, instanceA, false, 0, false)
	require.NoError(t, err)
}

func (s *StoreTest) TestDeleteExpiredLocks(t *testing.T, storage SyncStorage) {
	userID := uuid.New().String()
	lockName := "test_lock"
	instanceID := uuid.New().String()

	// Acquire lock with 1 second TTL
	err := storage.SetLock(context.Background(), userID, lockName, instanceID, true, 1, false)
	require.NoError(t, err)

	// Wait for expiry
	time.Sleep(2 * time.Second)

	// Delete expired locks
	err = storage.DeleteExpiredLocks(context.Background())
	require.NoError(t, err)

	// Verify it's gone
	locked, err := storage.HasActiveLock(context.Background(), userID, lockName)
	require.NoError(t, err)
	require.False(t, locked)
}
