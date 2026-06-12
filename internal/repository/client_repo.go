package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// ClientRepository provides database access for client (tenant) records.
type ClientRepository struct {
	db *sqlx.DB
}

// NewClientRepository creates a ClientRepository backed by db.
func NewClientRepository(db *sqlx.DB) *ClientRepository {
	return &ClientRepository{db: db}
}

// GetByID returns the client with the given primary key.
func (r *ClientRepository) GetByID(ctx context.Context, id string) (client *Client, err error) {
	var c Client
	err = r.db.GetContext(ctx, &c, `SELECT * FROM clients WHERE id = ? LIMIT 1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ClientRepository.GetByID: %w", err)
	}
	return &c, nil
}

// GetByAPIKey returns the active client whose api_key matches.
func (r *ClientRepository) GetByAPIKey(ctx context.Context, apiKey string) (client *Client, err error) {
	var c Client
	err = r.db.GetContext(ctx, &c,
		`SELECT * FROM clients WHERE api_key = ? AND is_active = 1 LIMIT 1`, apiKey)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ClientRepository.GetByAPIKey: %w", err)
	}
	return &c, nil
}

// GetBySMTPUsername returns the active client whose smtp_username matches.
func (r *ClientRepository) GetBySMTPUsername(ctx context.Context, username string) (client *Client, err error) {
	var c Client
	err = r.db.GetContext(ctx, &c,
		`SELECT * FROM clients WHERE smtp_username = ? AND is_active = 1 LIMIT 1`, username)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ClientRepository.GetBySMTPUsername: %w", err)
	}
	return &c, nil
}

// List returns all clients ordered by creation time, newest first.
func (r *ClientRepository) List(ctx context.Context) (clients []Client, err error) {
	if err = r.db.SelectContext(ctx, &clients,
		`SELECT * FROM clients ORDER BY created_at DESC`); err != nil {
		return nil, fmt.Errorf("ClientRepository.List: %w", err)
	}
	return clients, nil
}

// Create inserts a new client row. Returns ErrDuplicate if smtp_username or api_key
// already exists.
func (r *ClientRepository) Create(ctx context.Context, c *Client) error {
	_, err := r.db.NamedExecContext(ctx, `
		INSERT INTO clients
			(id, name, smtp_username, smtp_password_hash, api_key,
			 hourly_limit, monthly_limit, is_active)
		VALUES
			(:id, :name, :smtp_username, :smtp_password_hash, :api_key,
			 :hourly_limit, :monthly_limit, :is_active)`, c)
	if isDuplicateKeyErr(err) {
		return fmt.Errorf("ClientRepository.Create: %w", ErrDuplicate)
	}
	if err != nil {
		return fmt.Errorf("ClientRepository.Create: %w", err)
	}
	return nil
}

// Update saves mutable client fields: name, hourly_limit, monthly_limit, is_active.
func (r *ClientRepository) Update(ctx context.Context, c *Client) error {
	_, err := r.db.NamedExecContext(ctx, `
		UPDATE clients
		SET name          = :name,
		    hourly_limit  = :hourly_limit,
		    monthly_limit = :monthly_limit,
		    is_active     = :is_active
		WHERE id = :id`, c)
	if err != nil {
		return fmt.Errorf("ClientRepository.Update: %w", err)
	}
	return nil
}

// UpdateAPIKey replaces a client's api_key. Returns ErrDuplicate on collision.
func (r *ClientRepository) UpdateAPIKey(ctx context.Context, id, newKey string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE clients SET api_key = ? WHERE id = ?`, newKey, id)
	if isDuplicateKeyErr(err) {
		return fmt.Errorf("ClientRepository.UpdateAPIKey: %w", ErrDuplicate)
	}
	if err != nil {
		return fmt.Errorf("ClientRepository.UpdateAPIKey: %w", err)
	}
	return nil
}

// UpdatePassword replaces a client's smtp_password_hash (supply a bcrypt hash).
func (r *ClientRepository) UpdatePassword(ctx context.Context, id, passwordHash string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE clients SET smtp_password_hash = ? WHERE id = ?`, passwordHash, id)
	if err != nil {
		return fmt.Errorf("ClientRepository.UpdatePassword: %w", err)
	}
	return nil
}

// UpdateLimits sets per-client rate-limit thresholds.
func (r *ClientRepository) UpdateLimits(ctx context.Context, id string, hourly, monthly int) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE clients SET hourly_limit = ?, monthly_limit = ? WHERE id = ?`,
		hourly, monthly, id)
	if err != nil {
		return fmt.Errorf("ClientRepository.UpdateLimits: %w", err)
	}
	return nil
}

// Deactivate sets is_active = 0 without deleting the record.
func (r *ClientRepository) Deactivate(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE clients SET is_active = 0 WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("ClientRepository.Deactivate: %w", err)
	}
	return nil
}

// Delete permanently removes a client row.
func (r *ClientRepository) Delete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM clients WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("ClientRepository.Delete: %w", err)
	}
	return nil
}
