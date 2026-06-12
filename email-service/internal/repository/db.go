package repository

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"email-service/internal/config"
)

// NewDB opens and verifies a MySQL connection using cfg. It retries the ping
// with exponential backoff to accommodate docker-compose startup ordering.
func NewDB(cfg config.DatabaseConfig) (db *sqlx.DB, err error) {
	db, err = sqlx.Open("mysql", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("open mysql (%s): %w", cfg.SafeDSN(), err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	const maxAttempts = 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = db.Ping(); err == nil {
			return db, nil
		}
		if attempt == maxAttempts {
			break
		}
		time.Sleep(time.Duration(attempt*attempt) * time.Second)
	}

	db.Close()
	return nil, fmt.Errorf("ping mysql after %d attempts (%s): %w", maxAttempts, cfg.SafeDSN(), err)
}
