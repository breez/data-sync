package store

import (
	"context"
	"errors"
)

var ErrSetConflict = errors.New("set conflict")

type StoredRecord struct {
	Id       string
	Data     []byte
	Revision int64
}
type SyncStorage interface {
	SetRecord(ctx context.Context, userID, id string, data []byte, existingRevision int64) (int64, error)
	ListChanges(ctx context.Context, userID string, sinceRevision int64) ([]StoredRecord, error)
}
