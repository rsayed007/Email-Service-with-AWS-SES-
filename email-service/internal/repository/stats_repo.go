package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// StatsRepository provides database access for per-client daily send metrics.
type StatsRepository struct {
	db *sqlx.DB
}

// NewStatsRepository creates a StatsRepository backed by db.
func NewStatsRepository(db *sqlx.DB) *StatsRepository {
	return &StatsRepository{db: db}
}

// allowedStatColumns is the closed set of columns that IncrementStat may touch.
// It is declared as a package-level variable (not a map literal) so the compiler
// can verify exhaustiveness in tests.
var allowedStatColumns = map[string]struct{}{
	"sent":       {},
	"delivered":  {},
	"opened":     {},
	"clicked":    {},
	"bounced":    {},
	"complained": {},
}

// IncrementStat atomically increments column by 1 for (clientID, date), creating
// the row if it does not yet exist. column must be one of the allowedStatColumns.
func (r *StatsRepository) IncrementStat(ctx context.Context, clientID string, date time.Time, column string) (err error) {
	if _, ok := allowedStatColumns[column]; !ok {
		return fmt.Errorf("StatsRepository.IncrementStat: unknown column %q", column)
	}

	d := date.Format("2006-01-02")
	// fmt.Sprintf is safe here because column is validated against an allowlist.
	query := fmt.Sprintf(`
		INSERT INTO email_daily_stats (client_id, date, %s)
		VALUES (?, ?, 1)
		ON DUPLICATE KEY UPDATE %s = %s + 1`, column, column, column)

	if _, err = r.db.ExecContext(ctx, query, clientID, d); err != nil {
		return fmt.Errorf("StatsRepository.IncrementStat(%s): %w", column, err)
	}
	return nil
}

// GetRange returns per-day stats for clientID between from and to inclusive,
// ordered by date ascending.
func (r *StatsRepository) GetRange(ctx context.Context, clientID string, from, to time.Time) (stats []EmailDailyStat, err error) {
	err = r.db.SelectContext(ctx, &stats, `
		SELECT *
		FROM   email_daily_stats
		WHERE  client_id = ?
		  AND  date BETWEEN ? AND ?
		ORDER  BY date ASC`,
		clientID,
		from.Format("2006-01-02"),
		to.Format("2006-01-02"),
	)
	if err != nil {
		return nil, fmt.Errorf("StatsRepository.GetRange: %w", err)
	}
	return stats, nil
}

// GetSummary returns column totals aggregated over [from, to] for clientID.
// Useful for dashboard widgets that need a single row of totals.
func (r *StatsRepository) GetSummary(ctx context.Context, clientID string, from, to time.Time) (summary StatsSummary, err error) {
	err = r.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(sent),       0),
			COALESCE(SUM(delivered),  0),
			COALESCE(SUM(opened),     0),
			COALESCE(SUM(clicked),    0),
			COALESCE(SUM(bounced),    0),
			COALESCE(SUM(complained), 0)
		FROM  email_daily_stats
		WHERE client_id = ?
		  AND date BETWEEN ? AND ?`,
		clientID,
		from.Format("2006-01-02"),
		to.Format("2006-01-02"),
	).Scan(
		&summary.Sent,
		&summary.Delivered,
		&summary.Opened,
		&summary.Clicked,
		&summary.Bounced,
		&summary.Complained,
	)
	if err != nil {
		return StatsSummary{}, fmt.Errorf("StatsRepository.GetSummary: %w", err)
	}
	return summary, nil
}
