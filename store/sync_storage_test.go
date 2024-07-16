package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddRecords(t *testing.T) {
	storage, err := Connect("file:testaddrecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newVersion, err := storage.SetRecord(context.Background(), "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newVersion, int64(1))

	newVersion, err = storage.SetRecord(context.Background(), "a2", []byte("data2"), 0)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newVersion, int64(2))

	records, err := storage.ListChanges(context.Background(), 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{RecordID: "a1", Data: []byte("data1"), Version: 1},
		{RecordID: "a2", Data: []byte("data2"), Version: 2},
	})
}

func TestUpdateRecords(t *testing.T) {
	storage, err := Connect("file:testupdaterecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newVersion, err := storage.SetRecord(context.Background(), "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newVersion, int64(1))

	newVersion, err = storage.SetRecord(context.Background(), "a1", []byte("data2"), 1)
	require.NoError(t, err, "failed to call SetRecord a2")
	require.Equal(t, newVersion, int64(2))

	records, err := storage.ListChanges(context.Background(), 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, records, []StoredRecord{
		{RecordID: "a1", Data: []byte("data2"), Version: 2},
	})
}

func TestConflict(t *testing.T) {
	storage, err := Connect("file:testconflict?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	newVersion, err := storage.SetRecord(context.Background(), "a1", []byte("data1"), 0)
	require.NoError(t, err, "failed to call SetRecord a1")
	require.Equal(t, newVersion, int64(1))

	_, err = storage.SetRecord(context.Background(), "a1", []byte("data2"), 0)
	require.Error(t, err, "should have return with error")
	require.Equal(t, err, ErrSetConflict)
}
