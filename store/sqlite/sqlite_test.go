package sqlite

import (
	"context"
	"testing"

	"github.com/breez/data-sync/store"
	_ "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/stretchr/testify/require"
)

const testStoreID = "teststoreid"

func TestAddRecords(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testaddrecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	newRevision, err = storage.SetRecord(context.Background(), testStoreID, "a2", []byte("data2"), 0)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, int64(2))

	records, err := storage.ListChanges(context.Background(), testStoreID, 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []store.StoredRecord{
		{Id: "a1", Data: []byte("data1"), Revision: 1},
		{Id: "a2", Data: []byte("data2"), Revision: 2},
	})
}

func TestUpdateRecords(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testupdaterecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	newRevision, err = storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data2"), 1)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, int64(2))

	records, err := storage.ListChanges(context.Background(), testStoreID, 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []store.StoredRecord{
		{Id: "a1", Data: []byte("data2"), Revision: 2},
	})
}

func TestConflict(t *testing.T) {
	storage, err := NewSQLiteSyncStorage("file:testconflicts?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newRevision, err := storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	_, err = storage.SetRecord(context.Background(), testStoreID, "a1", []byte("data2"), 0)
	require.Error(t, err, "should have return with error")
	require.Equal(t, err, store.ErrSetConflict)
}
