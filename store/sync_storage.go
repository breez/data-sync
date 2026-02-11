package store

import (
	"context"
	"errors"
)

var ErrSetConflict = errors.New("set conflict")

type StoredRecord struct {
	Id            string
	Data          []byte
	Revision      uint64
	SchemaVersion string
}
type SyncStorage interface {
	SetRecord(ctx context.Context, userID, id string, data []byte, existingRevision uint64, schemaVersion string) (uint64, error)
	ListChanges(ctx context.Context, userID string, sinceRevision uint64) ([]StoredRecord, error)
	SetLock(ctx context.Context, userID, lockName, instanceID string, acquire bool, ttlSeconds uint32) error
	HasActiveLock(ctx context.Context, userID, lockName string) (bool, error)
	DeleteExpiredLocks(ctx context.Context) error
}
