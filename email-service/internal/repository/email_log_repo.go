package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// EmailLogRepository provides database access for email send records.
type EmailLogRepository struct {
	db *sqlx.DB
}

// NewEmailLogRepository creates an EmailLogRepository backed by db.
func NewEmailLogRepository(db *sqlx.DB) *EmailLogRepository {
	return &EmailLogRepository{db: db}
}

// Create inserts a new email_log row in StatusQueued state.
func (r *EmailLogRepository) Create(ctx context.Context, log *EmailLog) error {
	_, err := r.db.NamedExecContext(ctx, `
		INSERT INTO email_logs
			(id, client_id, from_email, to_email, subject, status)
		VALUES
			(:id, :client_id, :from_email, :to_email, :subject, :status)`, log)
	if err != nil {
		return fmt.Errorf("EmailLogRepository.Create: %w", err)
	}
	return nil
}

// GetByID returns a single EmailLog identified by its UUID.
func (r *EmailLogRepository) GetByID(ctx context.Context, id string) (log *EmailLog, err error) {
	var l EmailLog
	err = r.db.GetContext(ctx, &l, `SELECT * FROM email_logs WHERE id = ? LIMIT 1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("EmailLogRepository.GetByID: %w", err)
	}
	return &l, nil
}

// GetByAWSMessageID returns the email log whose aws_message_id matches.
// Used to correlate SNS delivery/bounce/complaint events.
func (r *EmailLogRepository) GetByAWSMessageID(ctx context.Context, msgID string) (log *EmailLog, err error) {
	var l EmailLog
	err = r.db.GetContext(ctx, &l,
		`SELECT * FROM email_logs WHERE aws_message_id = ? LIMIT 1`, msgID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("EmailLogRepository.GetByAWSMessageID: %w", err)
	}
	return &l, nil
}

// UpdateStatus advances an email log's status and stamps the matching timestamp column.
// For StatusOpened the timestamp is set only on first occurrence (COALESCE).
// For StatusClicked the timestamp is set only on first occurrence (COALESCE).
// For StatusFailed / StatusComplained only the status column is updated.
func (r *EmailLogRepository) UpdateStatus(ctx context.Context, id, status string) (err error) {
	now := time.Now().UTC()

	switch status {
	case StatusSent:
		_, err = r.db.ExecContext(ctx,
			`UPDATE email_logs SET status = ?, sent_at = ? WHERE id = ?`,
			status, now, id)

	case StatusDelivered:
		_, err = r.db.ExecContext(ctx,
			`UPDATE email_logs SET status = ?, delivered_at = ? WHERE id = ?`,
			status, now, id)

	case StatusOpened:
		// Don't downgrade from a terminal state; record first open time only.
		_, err = r.db.ExecContext(ctx, `
			UPDATE email_logs
			SET status    = CASE WHEN status IN (?,?,?) THEN status ELSE ? END,
			    opened_at = COALESCE(opened_at, ?)
			WHERE id = ?`,
			StatusClicked, StatusBounced, StatusComplained,
			status, now, id)

	case StatusClicked:
		// Record first click time only; clicked is a terminal engagement state.
		_, err = r.db.ExecContext(ctx,
			`UPDATE email_logs SET status = ?, clicked_at = COALESCE(clicked_at, ?) WHERE id = ?`,
			status, now, id)

	case StatusBounced:
		_, err = r.db.ExecContext(ctx,
			`UPDATE email_logs SET status = ?, bounced_at = ? WHERE id = ?`,
			status, now, id)

	default:
		// StatusFailed, StatusComplained, StatusQueued — status column only.
		_, err = r.db.ExecContext(ctx,
			`UPDATE email_logs SET status = ? WHERE id = ?`, status, id)
	}

	if err != nil {
		return fmt.Errorf("EmailLogRepository.UpdateStatus(%s): %w", status, err)
	}
	return nil
}

// SetAWSMessageID stores the message ID returned by SES after a successful send.
func (r *EmailLogRepository) SetAWSMessageID(ctx context.Context, id, awsMessageID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE email_logs SET aws_message_id = ? WHERE id = ?`, awsMessageID, id)
	if err != nil {
		return fmt.Errorf("EmailLogRepository.SetAWSMessageID: %w", err)
	}
	return nil
}

// LogFilter is the parameter object for List and Count queries.
type LogFilter struct {
	ClientID  string
	Status    string
	FromEmail string
	ToEmail   string
	Limit     int
	Offset    int
}

// List returns email logs matching f, newest first.
func (r *EmailLogRepository) List(ctx context.Context, f LogFilter) (logs []EmailLog, err error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}

	query, args := buildLogQuery(`SELECT *`, f)
	query += ` ORDER BY created_at DESC LIMIT ? OFFSET ?`
	args = append(args, f.Limit, f.Offset)

	if err = r.db.SelectContext(ctx, &logs, query, args...); err != nil {
		return nil, fmt.Errorf("EmailLogRepository.List: %w", err)
	}
	return logs, nil
}

// Count returns the total number of email logs matching f, for pagination metadata.
func (r *EmailLogRepository) Count(ctx context.Context, f LogFilter) (count int64, err error) {
	query, args := buildLogQuery(`SELECT COUNT(*)`, f)
	if err = r.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("EmailLogRepository.Count: %w", err)
	}
	return count, nil
}

// buildLogQuery constructs the FROM/WHERE portion shared by List and Count.
func buildLogQuery(selectClause string, f LogFilter) (string, []interface{}) {
	var conds []string
	var args []interface{}

	conds = append(conds, "client_id = ?")
	args = append(args, f.ClientID)

	if f.Status != "" {
		conds = append(conds, "status = ?")
		args = append(args, f.Status)
	}
	if f.FromEmail != "" {
		conds = append(conds, "from_email = ?")
		args = append(args, f.FromEmail)
	}
	if f.ToEmail != "" {
		conds = append(conds, "to_email = ?")
		args = append(args, f.ToEmail)
	}

	query := selectClause + " FROM email_logs WHERE " + strings.Join(conds, " AND ")
	return query, args
}
