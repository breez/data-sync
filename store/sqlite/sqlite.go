package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	"github.com/breez/data-sync/store"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/golang-migrate/migrate/v4/source/pkger"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type SQLiteSyncStorage struct {
	db *sql.DB
}

func NewSQLiteSyncStorage(file string) (*SQLiteSyncStorage, error) {
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite3 database %w", err)
	}

	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to create migration driver %w", err)
	}

	migrationDriver, err := iofs.New(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to create migration driver %w", err)
	}

	m, err := migrate.NewWithInstance(
		"iofs", migrationDriver,
		file, driver)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate migrations %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return nil, fmt.Errorf("failed to run migrations %w", err)
	}
	return &SQLiteSyncStorage{db: db}, nil
}

func (s *SQLiteSyncStorage) SetRecord(ctx context.Context, userID, id string, data []byte, existingRevision uint64, schemaVersion string) (uint64, error) {

	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// check that the existing revision is the same as the one we expect
	var revision uint64
	err = tx.QueryRow("SELECT revision FROM records WHERE user_id = ? AND id = ?", userID, id).Scan(&revision)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("failed to get record's latest revision: %w", err)
		}
		if existingRevision != revision {
			return 0, store.ErrSetConflict
		}
	}

	// get the store's last revision
	var newRevision uint64
	err = tx.QueryRow("SELECT revision FROM user_revisions WHERE user_id = ?", userID).Scan(&newRevision)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("failed to get store's latest revision: %w", err)
		}
	}
	if newRevision == 0 {
		err := tx.QueryRow("INSERT INTO user_revisions (user_id, revision) VALUES (?, ?) RETURNING revision", userID, 1).Scan(&newRevision)
		if err != nil {
			return 0, fmt.Errorf("failed to insert store's revision: %w", err)
		}
	} else {
		err := tx.QueryRow("UPDATE user_revisions SET revision = revision + 1 WHERE user_id = ? RETURNING revision", userID).Scan(&newRevision)
		if err != nil {
			return 0, fmt.Errorf("failed to update store's revision: %w", err)
		}
	}

	_, err = tx.Exec("INSERT OR REPLACE INTO records (user_id, id, data, revision, schema_version) VALUES (?, ?, ?, ?, ?) returning revision", userID, id, data, newRevision, schemaVersion)
	if err != nil {
		return 0, fmt.Errorf("failed to insert record: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return newRevision, nil
}

func (s *SQLiteSyncStorage) SetLock(ctx context.Context, userID, lockName, instanceID string, acquire bool, ttlSeconds uint32, exclusive bool) error {
	if !acquire {
		_, err := s.db.ExecContext(ctx, "DELETE FROM locks WHERE user_id = ? AND lock_name = ? AND instance_id = ?", userID, lockName, instanceID)
		if err != nil {
			return fmt.Errorf("failed to release lock: %w", err)
		}
		return nil
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	if exclusive {
		// Exclusive acquire: reject if any OTHER instance holds an active lock
		var exists int
		err = tx.QueryRowContext(ctx, "SELECT 1 FROM locks WHERE user_id = ? AND lock_name = ? AND instance_id != ? AND expires_at > ? LIMIT 1", userID, lockName, instanceID, now).Scan(&exists)
		if err == nil {
			return store.ErrLockHeld
		}
		if err != sql.ErrNoRows {
			return fmt.Errorf("failed to check existing locks: %w", err)
		}
	} else {
		// Non-exclusive acquire: reject if an exclusive lock exists from ANY other instance
		var exists int
		err = tx.QueryRowContext(ctx, "SELECT 1 FROM locks WHERE user_id = ? AND lock_name = ? AND instance_id != ? AND exclusive = 1 AND expires_at > ? LIMIT 1", userID, lockName, instanceID, now).Scan(&exists)
		if err == nil {
			return store.ErrLockHeld
		}
		if err != sql.ErrNoRows {
			return fmt.Errorf("failed to check exclusive locks: %w", err)
		}
	}

	expiresAt := time.Now().Unix() + int64(ttlSeconds)
	exclusiveInt := 0
	if exclusive {
		exclusiveInt = 1
	}
	_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO locks (user_id, lock_name, instance_id, expires_at, exclusive) VALUES (?, ?, ?, ?, ?)", userID, lockName, instanceID, expiresAt, exclusiveInt)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	return tx.Commit()
}

func (s *SQLiteSyncStorage) HasActiveLock(ctx context.Context, userID, lockName string) (bool, error) {
	now := time.Now().Unix()
	var exists int
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM locks WHERE user_id = ? AND lock_name = ? AND expires_at > ? LIMIT 1", userID, lockName, now).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check lock: %w", err)
	}
	return true, nil
}

func (s *SQLiteSyncStorage) DeleteExpiredLocks(ctx context.Context) error {
	now := time.Now().Unix()
	_, err := s.db.ExecContext(ctx, "DELETE FROM locks WHERE expires_at <= ?", now)
	if err != nil {
		return fmt.Errorf("failed to delete expired locks: %w", err)
	}
	return nil
}

func (s *SQLiteSyncStorage) ListChanges(ctx context.Context, userID string, sinceRevision uint64) ([]store.StoredRecord, error) {

	rows, err := s.db.Query("SELECT id, data, revision, schema_version FROM records WHERE user_id = ? AND revision > ?", userID, sinceRevision)
	if err != nil {
		return nil, fmt.Errorf("failed to query records: %w", err)
	}
	defer rows.Close()

	records := make([]store.StoredRecord, 0)
	for rows.Next() {
		record := store.StoredRecord{}
		err = rows.Scan(&record.Id, &record.Data, &record.Revision, &record.SchemaVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		records = append(records, record)
	}
	return records, nil
}
