package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type EmailLogRepository struct {
	db *sqlx.DB
}

func NewEmailLogRepository(db *sqlx.DB) *EmailLogRepository {
	return &EmailLogRepository{db: db}
}

func (r *EmailLogRepository) Create(ctx context.Context, log *EmailLog) error {
	_, err := r.db.NamedExecContext(ctx, `
		INSERT INTO email_logs
			(id, client_id, from_email, to_email, subject, status)
		VALUES
			(:id, :client_id, :from_email, :to_email, :subject, :status)`, log)
	if err != nil {
		return fmt.Errorf("create email log: %w", err)
	}
	return nil
}

func (r *EmailLogRepository) GetByID(ctx context.Context, id string) (*EmailLog, error) {
	var l EmailLog
	err := r.db.GetContext(ctx, &l, `SELECT * FROM email_logs WHERE id = ? LIMIT 1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get email log: %w", err)
	}
	return &l, nil
}

func (r *EmailLogRepository) GetByAWSMessageID(ctx context.Context, msgID string) (*EmailLog, error) {
	var l EmailLog
	err := r.db.GetContext(ctx, &l,
		`SELECT * FROM email_logs WHERE aws_message_id = ? LIMIT 1`, msgID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get email log by aws message id: %w", err)
	}
	return &l, nil
}

func (r *EmailLogRepository) UpdateStatus(ctx context.Context, id, status string) error {
	now := time.Now().UTC()
	var query string
	switch status {
	case StatusSent:
		query = `UPDATE email_logs SET status = ?, sent_at = ? WHERE id = ?`
	case StatusDelivered:
		query = `UPDATE email_logs SET status = ?, delivered_at = ? WHERE id = ?`
	case StatusOpened:
		query = `UPDATE email_logs SET status = ?, opened_at = ? WHERE id = ?`
	case StatusClicked:
		query = `UPDATE email_logs SET status = ?, clicked_at = ? WHERE id = ?`
	case StatusBounced:
		query = `UPDATE email_logs SET status = ?, bounced_at = ? WHERE id = ?`
	default:
		query = `UPDATE email_logs SET status = ?, sent_at = ? WHERE id = ?`
	}
	_, err := r.db.ExecContext(ctx, query, status, now, id)
	if err != nil {
		return fmt.Errorf("update email log status: %w", err)
	}
	return nil
}

func (r *EmailLogRepository) SetAWSMessageID(ctx context.Context, id, awsMessageID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE email_logs SET aws_message_id = ? WHERE id = ?`, awsMessageID, id)
	if err != nil {
		return fmt.Errorf("set aws message id: %w", err)
	}
	return nil
}

type LogFilter struct {
	ClientID string
	Status   string
	Limit    int
	Offset   int
}

func (r *EmailLogRepository) List(ctx context.Context, f LogFilter) ([]EmailLog, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	query := `SELECT * FROM email_logs WHERE client_id = ?`
	args := []interface{}{f.ClientID}
	if f.Status != "" {
		query += ` AND status = ?`
		args = append(args, f.Status)
	}
	query += ` ORDER BY created_at DESC LIMIT ? OFFSET ?`
	args = append(args, f.Limit, f.Offset)

	var logs []EmailLog
	if err := r.db.SelectContext(ctx, &logs, query, args...); err != nil {
		return nil, fmt.Errorf("list email logs: %w", err)
	}
	return logs, nil
}
