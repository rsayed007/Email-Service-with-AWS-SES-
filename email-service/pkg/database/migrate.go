// Package database provides database connection management and schema migration
// utilities for the email-service.
package database

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migmysql "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"email-service/internal/config"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrator wraps golang-migrate and exposes a small, explicit API.
type Migrator struct {
	m      *migrate.Migrate
	dbName string
}

// New constructs a Migrator using the embedded SQL files and the provided *sql.DB.
// The caller retains ownership of db; closing it before calling Migrator.Close
// will cause undefined behaviour.
func New(db *sql.DB, cfg config.DatabaseConfig) (*Migrator, error) {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("database.New: create iofs source: %w", err)
	}

	driver, err := migmysql.WithInstance(db, &migmysql.Config{
		DatabaseName: cfg.Database,
	})
	if err != nil {
		return nil, fmt.Errorf("database.New: create mysql driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, cfg.Database, driver)
	if err != nil {
		return nil, fmt.Errorf("database.New: init migrate: %w", err)
	}

	return &Migrator{m: m, dbName: cfg.Database}, nil
}

// Up applies every pending migration. Returns nil if there is nothing to do.
func (m *Migrator) Up() error {
	if err := m.m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("Migrator.Up: %w", err)
	}
	return nil
}

// Down rolls back every applied migration. Returns nil if the schema is already empty.
func (m *Migrator) Down() error {
	if err := m.m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("Migrator.Down: %w", err)
	}
	return nil
}

// Steps applies n migrations (positive = up, negative = down).
func (m *Migrator) Steps(n int) error {
	if err := m.m.Steps(n); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("Migrator.Steps(%d): %w", n, err)
	}
	return nil
}

// Version returns the current schema version and whether the database is dirty.
// Returns (0, false, nil) when no migrations have been applied yet.
func (m *Migrator) Version() (version uint, dirty bool, err error) {
	version, dirty, err = m.m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("Migrator.Version: %w", err)
	}
	return version, dirty, nil
}

// Close releases resources held by the source and database drivers.
// The underlying *sql.DB is NOT closed here; the caller is responsible.
func (m *Migrator) Close() error {
	srcErr, dbErr := m.m.Close()
	switch {
	case srcErr != nil && dbErr != nil:
		return fmt.Errorf("Migrator.Close: source: %v; db: %v", srcErr, dbErr)
	case srcErr != nil:
		return fmt.Errorf("Migrator.Close: source: %w", srcErr)
	case dbErr != nil:
		return fmt.Errorf("Migrator.Close: db: %w", dbErr)
	}
	return nil
}

// RunMigrations is a convenience wrapper that creates a Migrator, calls Up,
// logs the resulting schema version, and closes the migrator. The *sql.DB
// stays open for the rest of the application.
func RunMigrations(db *sql.DB, cfg config.DatabaseConfig) (err error) {
	mgr, err := New(db, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := mgr.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if err = mgr.Up(); err != nil {
		return err
	}

	version, dirty, err := mgr.Version()
	if err != nil {
		return err
	}

	if dirty {
		return fmt.Errorf("database is in a dirty state at version %d; manual intervention required", version)
	}
	_ = version // caller can log via slog if desired
	return nil
}
