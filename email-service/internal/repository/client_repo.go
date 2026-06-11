package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

var ErrNotFound = errors.New("not found")

type ClientRepository struct {
	db *sqlx.DB
}

func NewClientRepository(db *sqlx.DB) *ClientRepository {
	return &ClientRepository{db: db}
}

func (r *ClientRepository) GetByAPIKey(ctx context.Context, apiKey string) (*Client, error) {
	var c Client
	err := r.db.GetContext(ctx, &c,
		`SELECT * FROM clients WHERE api_key = ? AND is_active = 1 LIMIT 1`, apiKey)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get client by api key: %w", err)
	}
	return &c, nil
}

func (r *ClientRepository) GetBySMTPUsername(ctx context.Context, username string) (*Client, error) {
	var c Client
	err := r.db.GetContext(ctx, &c,
		`SELECT * FROM clients WHERE smtp_username = ? AND is_active = 1 LIMIT 1`, username)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get client by smtp username: %w", err)
	}
	return &c, nil
}

func (r *ClientRepository) GetByID(ctx context.Context, id string) (*Client, error) {
	var c Client
	err := r.db.GetContext(ctx, &c, `SELECT * FROM clients WHERE id = ? LIMIT 1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get client by id: %w", err)
	}
	return &c, nil
}

func (r *ClientRepository) Create(ctx context.Context, c *Client) error {
	_, err := r.db.NamedExecContext(ctx, `
		INSERT INTO clients
			(id, name, smtp_username, smtp_password_hash, api_key,
			 hourly_limit, monthly_limit, is_active)
		VALUES
			(:id, :name, :smtp_username, :smtp_password_hash, :api_key,
			 :hourly_limit, :monthly_limit, :is_active)`, c)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	return nil
}

func (r *ClientRepository) Update(ctx context.Context, c *Client) error {
	_, err := r.db.NamedExecContext(ctx, `
		UPDATE clients SET
			name = :name,
			hourly_limit = :hourly_limit,
			monthly_limit = :monthly_limit,
			is_active = :is_active
		WHERE id = :id`, c)
	if err != nil {
		return fmt.Errorf("update client: %w", err)
	}
	return nil
}

func (r *ClientRepository) List(ctx context.Context) ([]Client, error) {
	var clients []Client
	if err := r.db.SelectContext(ctx, &clients, `SELECT * FROM clients ORDER BY created_at DESC`); err != nil {
		return nil, fmt.Errorf("list clients: %w", err)
	}
	return clients, nil
}
