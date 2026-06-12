package repository

import (
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Sentinel errors returned by repository methods.
var (
	ErrNotFound  = errors.New("not found")
	ErrDuplicate = errors.New("duplicate entry")
)

// isDuplicateKeyErr reports whether err is a MySQL duplicate-key violation (1062).
func isDuplicateKeyErr(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

// Client represents a tenant of the email service.
type Client struct {
	ID               string    `db:"id"                  json:"id"`
	Name             string    `db:"name"                json:"name"`
	SMTPUsername     string    `db:"smtp_username"       json:"smtp_username"`
	SMTPPasswordHash string    `db:"smtp_password_hash"  json:"-"`
	APIKey           string    `db:"api_key"             json:"api_key,omitempty"`
	HourlyLimit      int       `db:"hourly_limit"        json:"hourly_limit"`
	MonthlyLimit     int       `db:"monthly_limit"       json:"monthly_limit"`
	IsActive         bool      `db:"is_active"           json:"is_active"`
	CreatedAt        time.Time `db:"created_at"          json:"created_at"`
	UpdatedAt        time.Time `db:"updated_at"          json:"updated_at"`
}

// EmailLog records a single email send attempt and its lifecycle events.
type EmailLog struct {
	ID           string     `db:"id"             json:"id"`
	ClientID     string     `db:"client_id"      json:"client_id"`
	AWSMessageID *string    `db:"aws_message_id" json:"aws_message_id,omitempty"`
	FromEmail    string     `db:"from_email"     json:"from_email"`
	ToEmail      string     `db:"to_email"       json:"to_email"`
	Subject      string     `db:"subject"        json:"subject"`
	Status       string     `db:"status"         json:"status"`
	SentAt       *time.Time `db:"sent_at"        json:"sent_at,omitempty"`
	DeliveredAt  *time.Time `db:"delivered_at"   json:"delivered_at,omitempty"`
	OpenedAt     *time.Time `db:"opened_at"      json:"opened_at,omitempty"`
	ClickedAt    *time.Time `db:"clicked_at"     json:"clicked_at,omitempty"`
	BouncedAt    *time.Time `db:"bounced_at"     json:"bounced_at,omitempty"`
	CreatedAt    time.Time  `db:"created_at"     json:"created_at"`
}

// EmailDailyStat holds aggregated per-client send metrics for a single calendar day.
type EmailDailyStat struct {
	ClientID   string    `db:"client_id"  json:"client_id"`
	Date       time.Time `db:"date"       json:"date"`
	Sent       int       `db:"sent"       json:"sent"`
	Delivered  int       `db:"delivered"  json:"delivered"`
	Opened     int       `db:"opened"     json:"opened"`
	Clicked    int       `db:"clicked"    json:"clicked"`
	Bounced    int       `db:"bounced"    json:"bounced"`
	Complained int       `db:"complained" json:"complained"`
}

// StatsSummary holds totals aggregated over a date range.
type StatsSummary struct {
	Sent       int `db:"sent"       json:"sent"`
	Delivered  int `db:"delivered"  json:"delivered"`
	Opened     int `db:"opened"     json:"opened"`
	Clicked    int `db:"clicked"    json:"clicked"`
	Bounced    int `db:"bounced"    json:"bounced"`
	Complained int `db:"complained" json:"complained"`
}

// BlacklistedEmail is an address that must not receive email from a specific client.
type BlacklistedEmail struct {
	ID        int64     `db:"id"         json:"id"`
	ClientID  string    `db:"client_id"  json:"client_id"`
	Email     string    `db:"email"      json:"email"`
	Reason    string    `db:"reason"     json:"reason"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// Email status constants used across the repository and service layers.
const (
	StatusQueued     = "queued"
	StatusSent       = "sent"
	StatusDelivered  = "delivered"
	StatusOpened     = "opened"
	StatusClicked    = "clicked"
	StatusBounced    = "bounced"      // kept for legacy reads; new writes use the typed variants
	StatusHardBounced = "hard_bounced" // permanent bounce — recipient address blacklisted
	StatusSoftBounced = "soft_bounced" // transient bounce — may succeed on retry
	StatusComplained = "complained"
	StatusFailed     = "failed"
)
