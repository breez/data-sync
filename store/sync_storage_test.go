package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddRecords(t *testing.T) {
	storage, err := Connect("file:testaddrecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newRevision, err := storage.SetRecord(context.Background(), "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	newRevision, err = storage.SetRecord(context.Background(), "a2", []byte("data2"), 0)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, int64(2))

	records, err := storage.ListChanges(context.Background(), 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{Id: "a1", Data: []byte("data1"), Revision: 1},
		{Id: "a2", Data: []byte("data2"), Revision: 2},
	})
}

func TestUpdateRecords(t *testing.T) {
	storage, err := Connect("file:testupdaterecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newRevision, err := storage.SetRecord(context.Background(), "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	newRevision, err = storage.SetRecord(context.Background(), "a1", []byte("data2"), 1)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newRevision, int64(2))

	records, err := storage.ListChanges(context.Background(), 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{Id: "a1", Data: []byte("data2"), Revision: 2},
	})
}

func TestConflict(t *testing.T) {
	storage, err := Connect("file:testconflict?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newRevision, err := storage.SetRecord(context.Background(), "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newRevision, int64(1))

	_, err = storage.SetRecord(context.Background(), "a1", []byte("data2"), 0)
	require.Error(t, err, "should have return with error")
	require.Equal(t, err, ErrSetConflict)
}
