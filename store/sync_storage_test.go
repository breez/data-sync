package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddRecords(t *testing.T) {
	storage, err := Connect("file:testaddrecords?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	records := []StoredRecord{
		{Id: 0, Version: 0.1, Data: []byte("{}")},
		{Id: 1, Version: 0.1, Data: []byte("{}")},
	}

	newId, err := storage.SetRecord(context.Background(), records[0].Id, records[0].Version, records[0].Data)
	require.NoError(t, err, "failed to call SetRecord 0")
	require.Equal(t, newId, int64(0))

	newId, err = storage.SetRecord(context.Background(), records[1].Id, records[1].Version, records[1].Data)
	require.NoError(t, err, "failed to call SetRecord 1")
	require.Equal(t, newId, int64(1))

	fetchedRecords, err := storage.ListChanges(context.Background(), 0)
	require.NoError(t, err, "failed to call list changes")
	require.Equal(t, fetchedRecords, records)
}

func TestConflict(t *testing.T) {
	storage, err := Connect("file:testconflict?mode=memory&cache=shared")
	require.NoError(t, err, "failed to connect")

	record := StoredRecord{
		Id: 0, Version: 0.1, Data: []byte("{}"),
	}

	newId, err := storage.SetRecord(context.Background(), record.Id, record.Version, record.Data)
	require.NoError(t, err, "failed to call SetRecord 0")
	require.Equal(t, newId, int64(0))

	newId, err = storage.SetRecord(context.Background(), record.Id, record.Version, record.Data)
	require.Error(t, err, "Second call of SetRecord 0 is supposed to fail due to set conflict")
	require.Equal(t, err, ErrSetConflict)
}
