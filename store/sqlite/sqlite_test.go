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
