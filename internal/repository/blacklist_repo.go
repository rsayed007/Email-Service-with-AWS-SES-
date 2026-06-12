package repository

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// BlacklistRepository provides database access for per-client email blacklists.
type BlacklistRepository struct {
	db *sqlx.DB
}

// NewBlacklistRepository creates a BlacklistRepository backed by db.
func NewBlacklistRepository(db *sqlx.DB) *BlacklistRepository {
	return &BlacklistRepository{db: db}
}

// IsBlacklisted reports whether email is on clientID's blacklist.
func (r *BlacklistRepository) IsBlacklisted(ctx context.Context, clientID, email string) (blacklisted bool, err error) {
	var count int
	err = r.db.GetContext(ctx, &count,
		`SELECT COUNT(1) FROM blacklisted_emails WHERE client_id = ? AND email = ?`,
		clientID, email)
	if err != nil {
		return false, fmt.Errorf("BlacklistRepository.IsBlacklisted: %w", err)
	}
	return count > 0, nil
}

// Add inserts an entry into the blacklist. Silently ignores duplicates (INSERT IGNORE).
func (r *BlacklistRepository) Add(ctx context.Context, entry *BlacklistedEmail) error {
	_, err := r.db.NamedExecContext(ctx, `
		INSERT IGNORE INTO blacklisted_emails (client_id, email, reason)
		VALUES (:client_id, :email, :reason)`, entry)
	if err != nil {
		return fmt.Errorf("BlacklistRepository.Add: %w", err)
	}
	return nil
}

// Remove deletes a specific email from clientID's blacklist.
func (r *BlacklistRepository) Remove(ctx context.Context, clientID, email string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM blacklisted_emails WHERE client_id = ? AND email = ?`,
		clientID, email)
	if err != nil {
		return fmt.Errorf("BlacklistRepository.Remove: %w", err)
	}
	return nil
}

// List returns all blacklist entries for clientID, newest first.
func (r *BlacklistRepository) List(ctx context.Context, clientID string) (entries []BlacklistedEmail, err error) {
	err = r.db.SelectContext(ctx, &entries,
		`SELECT * FROM blacklisted_emails WHERE client_id = ? ORDER BY created_at DESC`,
		clientID)
	if err != nil {
		return nil, fmt.Errorf("BlacklistRepository.List: %w", err)
	}
	return entries, nil
}

// Count returns the number of blacklisted addresses for clientID.
func (r *BlacklistRepository) Count(ctx context.Context, clientID string) (count int64, err error) {
	err = r.db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM blacklisted_emails WHERE client_id = ?`, clientID)
	if err != nil {
		return 0, fmt.Errorf("BlacklistRepository.Count: %w", err)
	}
	return count, nil
}

// RemoveAll deletes every blacklist entry for clientID.
func (r *BlacklistRepository) RemoveAll(ctx context.Context, clientID string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM blacklisted_emails WHERE client_id = ?`, clientID)
	if err != nil {
		return fmt.Errorf("BlacklistRepository.RemoveAll: %w", err)
	}
	return nil
}
