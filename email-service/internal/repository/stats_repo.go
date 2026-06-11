package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type StatsRepository struct {
	db *sqlx.DB
}

func NewStatsRepository(db *sqlx.DB) *StatsRepository {
	return &StatsRepository{db: db}
}

// IncrementStat atomically increments a single column for a (client, date) row.
// The row is upserted on first write.
func (r *StatsRepository) IncrementStat(ctx context.Context, clientID string, date time.Time, column string) error {
	// Only allow known columns to prevent SQL injection.
	allowed := map[string]bool{
		"sent": true, "delivered": true, "opened": true,
		"clicked": true, "bounced": true, "complained": true,
	}
	if !allowed[column] {
		return fmt.Errorf("unknown stat column: %s", column)
	}
	d := date.Format("2006-01-02")
	query := fmt.Sprintf(`
		INSERT INTO email_daily_stats (client_id, date, %s)
		VALUES (?, ?, 1)
		ON DUPLICATE KEY UPDATE %s = %s + 1`, column, column, column)
	_, err := r.db.ExecContext(ctx, query, clientID, d)
	if err != nil {
		return fmt.Errorf("increment stat %s: %w", column, err)
	}
	return nil
}

func (r *StatsRepository) GetRange(ctx context.Context, clientID string, from, to time.Time) ([]EmailDailyStat, error) {
	var stats []EmailDailyStat
	err := r.db.SelectContext(ctx, &stats, `
		SELECT * FROM email_daily_stats
		WHERE client_id = ? AND date BETWEEN ? AND ?
		ORDER BY date ASC`,
		clientID, from.Format("2006-01-02"), to.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("get stats range: %w", err)
	}
	return stats, nil
}
