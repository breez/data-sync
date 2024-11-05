package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/breez/data-sync/store"
	"github.com/markbates/pkger"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/golang-migrate/migrate/v4/source/pkger"
	_ "github.com/mattn/go-sqlite3"
)

var _ = pkger.Include("/store/migrations")

type SQLiteSyncStorage struct {
	db *sql.DB
}

func NewSQLiteSyncStorage(file string) (*SQLiteSyncStorage, error) {
	needMigration := false
	if _, err := os.Stat(file); errors.Is(err, os.ErrNotExist) {
		needMigration = true
	}
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite3 database %w", err)
	}

	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to create migration driver %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"pkger:///store/sqlite/migrations",
		file, driver)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate migrations %w", err)
	}
	if needMigration {
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			return nil, fmt.Errorf("failed to run migrations %w", err)
		}
	}
	return &SQLiteSyncStorage{db: db}, nil
}

func (s *SQLiteSyncStorage) SetRecord(ctx context.Context, storeID, id string, data []byte, existingRevision int64) (int64, error) {

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// check that the existing revision is the same as the one we expect
	var revision int64
	err = tx.QueryRow("SELECT revision FROM records WHERE id = ?", id).Scan(&revision)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("failed to get record's latest revision: %w", err)
		}
		if existingRevision != revision {
			return 0, store.ErrSetConflict
		}
	}

	// get the store's last revision
	var newRevision int64
	err = tx.QueryRow("SELECT revision FROM store_revisions WHERE store_id = ?", storeID).Scan(&newRevision)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("failed to get store's latest revision: %w", err)
		}
	}
	if newRevision == 0 {
		err := tx.QueryRow("INSERT INTO store_revisions (store_id, revision) VALUES (?, ?) RETURNING revision", storeID, 1).Scan(&newRevision)
		if err != nil {
			return 0, fmt.Errorf("failed to insert store's revision: %w", err)
		}
	} else {
		err := tx.QueryRow("UPDATE store_revisions SET revision = revision + 1 WHERE store_id = ? RETURNING revision", storeID).Scan(&newRevision)
		if err != nil {
			return 0, fmt.Errorf("failed to update store's revision: %w", err)
		}
	}

	_, err = tx.Exec("INSERT OR REPLACE INTO records (store_id, id, data, revision) VALUES (?, ?, ?, ?) returning revision", storeID, id, data, newRevision)
	if err != nil {
		return 0, fmt.Errorf("failed to insert record: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return newRevision, nil
}

func (s *SQLiteSyncStorage) ListChanges(ctx context.Context, storeID string, sinceRevision int64) ([]store.StoredRecord, error) {

	rows, err := s.db.Query("SELECT id, data, revision FROM records WHERE store_id = ? AND revision > ?", storeID, sinceRevision)
	if err != nil {
		return nil, fmt.Errorf("failed to query records: %w", err)
	}
	defer rows.Close()

	records := make([]store.StoredRecord, 0)
	for rows.Next() {
		record := store.StoredRecord{}
		err = rows.Scan(&record.Id, &record.Data, &record.Revision)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		records = append(records, record)
	}
	return records, nil
}
