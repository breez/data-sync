package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/markbates/pkger"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/golang-migrate/migrate/v4/source/pkger"
	_ "github.com/mattn/go-sqlite3"
)

var ErrSetConflict = errors.New("set conflict")

type StoredRecord struct {
	Id      int64
	Version float32
	Data    []byte
}
type SyncStorage interface {
	SetRecord(ctx context.Context, userRecordId int64, version float32, data []byte) (int64, error)
	ListChanges(ctx context.Context, fromId int64) ([]StoredRecord, error)
}

type SQLiteSyncStorage struct {
	db *sql.DB
}

func Connect(file string) (*SQLiteSyncStorage, error) {
	if err := os.MkdirAll(path.Dir(file), 0700); err != nil {
		return nil, fmt.Errorf("failed to create databases directory %w", err)
	}
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite3 database %w", err)
	}

	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to create migration driver %w", err)
	}

	_ = pkger.Include("/store/migrations")
	m, err := migrate.NewWithDatabaseInstance(
		"pkger:///store/migrations",
		file, driver)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate migrations %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return nil, fmt.Errorf("failed to run migrations %w", err)

	}
	return &SQLiteSyncStorage{db: db}, nil
}

func (s *SQLiteSyncStorage) SetRecord(ctx context.Context, userRecordId int64, version float32, data []byte) (int64, error) {
	var latestRecordId int64
	err := s.db.QueryRow(`
		SELECT id 
		FROM RECORDS 
		ORDER BY id DESC
		LIMIT 1
	`).Scan(&latestRecordId)
	if err != sql.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("failed to currenet version %w", err)
		}
		if latestRecordId+1 != userRecordId {
			return 0, ErrSetConflict
		}
	}

	res, err := s.db.Exec("INSERT INTO RECORDS (version, data) VALUES (?, ?)", version, data)
	if err != nil {
		return 0, fmt.Errorf("failed to insert record: %w", err)

	}
	return res.LastInsertId()
}

func (s *SQLiteSyncStorage) ListChanges(ctx context.Context, fromId int64) ([]StoredRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, version, data 
		FROM RECORDS 
		WHERE id >= ? 
	`, fromId)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve changes: %w", err)
	}
	defer rows.Close()

	records := make([]StoredRecord, 0)
	for rows.Next() {
		record := StoredRecord{}
		err = rows.Scan(&record.Id, &record.Version, &record.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to scan %w", err)
		}
		records = append(records, record)
	}
	return records, nil
}
