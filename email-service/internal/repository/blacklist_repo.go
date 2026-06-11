package repository

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type BlacklistRepository struct {
	db *sqlx.DB
}

func NewBlacklistRepository(db *sqlx.DB) *BlacklistRepository {
	return &BlacklistRepository{db: db}
}

func (r *BlacklistRepository) IsBlacklisted(ctx context.Context, clientID, email string) (bool, error) {
	var count int
	err := r.db.GetContext(ctx, &count,
		`SELECT COUNT(1) FROM blacklisted_emails WHERE client_id = ? AND email = ?`,
		clientID, email)
	if err != nil {
		return false, fmt.Errorf("check blacklist: %w", err)
	}
	return count > 0, nil
}

func (r *BlacklistRepository) Add(ctx context.Context, entry *BlacklistedEmail) error {
	_, err := r.db.NamedExecContext(ctx, `
		INSERT IGNORE INTO blacklisted_emails (client_id, email, reason)
		VALUES (:client_id, :email, :reason)`, entry)
	if err != nil {
		return fmt.Errorf("add to blacklist: %w", err)
	}
	return nil
}

func (r *BlacklistRepository) Remove(ctx context.Context, clientID, email string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM blacklisted_emails WHERE client_id = ? AND email = ?`, clientID, email)
	if err != nil {
		return fmt.Errorf("remove from blacklist: %w", err)
	}
	return nil
}

func (r *BlacklistRepository) List(ctx context.Context, clientID string) ([]BlacklistedEmail, error) {
	var entries []BlacklistedEmail
	err := r.db.SelectContext(ctx, &entries,
		`SELECT * FROM blacklisted_emails WHERE client_id = ? ORDER BY created_at DESC`, clientID)
	if err != nil {
		return nil, fmt.Errorf("list blacklist: %w", err)
	}
	return entries, nil
}
