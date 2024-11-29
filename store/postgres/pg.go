package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/breez/data-sync/store"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/golang-migrate/migrate/v4/source/pkger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type PgSyncStorage struct {
	db *pgxpool.Pool
}

func NewPGSyncStorage(databaseURL string) (*PgSyncStorage, error) {

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite3 database %w", err)
	}
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to create migration driver %w", err)
	}

	migrationDriver, err := iofs.New(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to create migration driver %w", err)
	}

	m, err := migrate.NewWithInstance(
		"iofs", migrationDriver,
		"data-sync", driver)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate migrations %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return nil, fmt.Errorf("failed to run migrations %w", err)
	}

	pgxPool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New(%v): %w", databaseURL, err)
	}
	return &PgSyncStorage{db: pgxPool}, nil
}

func (s *PgSyncStorage) SetRecord(ctx context.Context, userID, id string, data []byte, existingRevision uint64, schemaVersion string) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	// check that the existing revision is the same as the one we expect
	var revision uint64
	err = tx.QueryRow(ctx, "SELECT revision FROM records WHERE user_id = $1 AND id = $2", userID, id).Scan(&revision)
	if err != pgx.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("failed to get record's latest revision: %w", err)
		}
		if existingRevision != revision {
			return 0, store.ErrSetConflict
		}
	}

	// get the store's last revision
	var newRevision uint64 = 0
	err = tx.QueryRow(ctx, "SELECT revision FROM user_revisions WHERE user_id = $1", userID).Scan(&newRevision)
	if err != pgx.ErrNoRows {
		if err != nil {
			return 0, fmt.Errorf("failed to get store's latest revision: %w", err)
		}
	}
	if newRevision == 0 {
		err := tx.QueryRow(ctx, "INSERT INTO user_revisions (user_id, revision) VALUES ($1, $2) RETURNING revision", userID, 1).Scan(&newRevision)
		if err != nil {
			return 0, fmt.Errorf("failed to insert store's revision: %w", err)
		}
	} else {
		err := tx.QueryRow(ctx, "UPDATE user_revisions SET revision = revision + 1 WHERE user_id = $1 RETURNING revision", userID).Scan(&newRevision)
		if err != nil {
			return 0, fmt.Errorf("failed to update store's revision: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO records (user_id, id, data, revision, schema_version) 
		VALUES ($1, $2, $3, $4, $5) 
		ON CONFLICT (user_id, id) DO UPDATE SET 
			data=EXCLUDED.data, 
			revision=EXCLUDED.revision, 
			schema_version=EXCLUDED.schema_version 
		RETURNING revision`,
		userID, id, data, newRevision, schemaVersion,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert record: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return newRevision, nil
}

func (s *PgSyncStorage) ListChanges(ctx context.Context, userID string, sinceRevision uint64) ([]store.StoredRecord, error) {

	rows, err := s.db.Query(ctx, "SELECT id, data, revision, schema_version FROM records WHERE user_id = $1 AND revision > $2", userID, sinceRevision)
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
