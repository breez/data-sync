package store

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type StoreTest struct{}

func (s *StoreTest) TestAddRecords(t *testing.T, storage SyncStorage) {

	testStoreID := uuid.New().String()
	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	newRevision, err = storage.SetRecord(context.Background(), testStoreID, "a2", []byte("data2"), 0)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, int64(2))

	records, err := storage.ListChanges(context.Background(), testStoreID, 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{Id: "a1", Data: []byte("data1"), Revision: 1},
		{Id: "a2", Data: []byte("data2"), Revision: 2},
	})

	// Test different store with same id
	anotherStoreID := uuid.New().String()
	newRev, err := storage.SetRecord(context.Background(), anotherStoreID, "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRev, int64(1))
}

func (s *StoreTest) TestUpdateRecords(t *testing.T, storage SyncStorage) {
	testStoreID := uuid.New().String()
	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	newRevision, err = storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data2"), 1)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, int64(2))

	records, err := storage.ListChanges(context.Background(), testStoreID, 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{Id: "a1", Data: []byte("data2"), Revision: 2},
	})
}

func (s *StoreTest) TestConflict(t *testing.T, storage SyncStorage) {
	testStoreID := uuid.New().String()
	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	_, err = storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data2"), 0)
	require.Error(t, err, "should have return with error")
	require.Equal(t, err, ErrSetConflict)
}
