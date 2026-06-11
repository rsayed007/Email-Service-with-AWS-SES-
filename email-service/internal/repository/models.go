package repository

import "time"

type Client struct {
	ID               string    `db:"id"`
	Name             string    `db:"name"`
	SMTPUsername     string    `db:"smtp_username"`
	SMTPPasswordHash string    `db:"smtp_password_hash"`
	APIKey           string    `db:"api_key"`
	HourlyLimit      int       `db:"hourly_limit"`
	MonthlyLimit     int       `db:"monthly_limit"`
	IsActive         bool      `db:"is_active"`
	CreatedAt        time.Time `db:"created_at"`
	UpdatedAt        time.Time `db:"updated_at"`
}

type EmailLog struct {
	ID            string     `db:"id"`
	ClientID      string     `db:"client_id"`
	AWSMessageID  *string    `db:"aws_message_id"`
	FromEmail     string     `db:"from_email"`
	ToEmail       string     `db:"to_email"`
	Subject       string     `db:"subject"`
	Status        string     `db:"status"`
	SentAt        *time.Time `db:"sent_at"`
	DeliveredAt   *time.Time `db:"delivered_at"`
	OpenedAt      *time.Time `db:"opened_at"`
	ClickedAt     *time.Time `db:"clicked_at"`
	BouncedAt     *time.Time `db:"bounced_at"`
	CreatedAt     time.Time  `db:"created_at"`
}

type EmailDailyStat struct {
	ClientID  string    `db:"client_id"`
	Date      time.Time `db:"date"`
	Sent      int       `db:"sent"`
	Delivered int       `db:"delivered"`
	Opened    int       `db:"opened"`
	Clicked   int       `db:"clicked"`
	Bounced   int       `db:"bounced"`
	Complained int      `db:"complained"`
}

type BlacklistedEmail struct {
	ID        int64     `db:"id"`
	ClientID  string    `db:"client_id"`
	Email     string    `db:"email"`
	Reason    string    `db:"reason"`
	CreatedAt time.Time `db:"created_at"`
}

// Email status constants
const (
	StatusQueued    = "queued"
	StatusSent      = "sent"
	StatusDelivered = "delivered"
	StatusOpened    = "opened"
	StatusClicked   = "clicked"
	StatusBounced   = "bounced"
	StatusComplained = "complained"
	StatusFailed    = "failed"
)
