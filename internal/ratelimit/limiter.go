// Package ratelimit — this file contains backward-compatible aliases retained
// during the migration from the original Limiter API to RateLimiter.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter is a type alias for RateLimiter.
// Deprecated: use RateLimiter directly.
type Limiter = RateLimiter

// Result is the legacy return type for CheckAndIncrement.
// Deprecated: use LimitResult.
type Result struct {
	Allowed   bool
	Remaining int64
	ResetAt   time.Time
}

// NewLimiter creates a RateLimiter. Deprecated: use NewRateLimiter.
func NewLimiter(rdb *redis.Client) *RateLimiter {
	return NewRateLimiter(rdb)
}

// CheckAndIncrement checks and increments both the hourly and monthly limits.
// Deprecated: use CheckAll for atomic dual-limit enforcement.
func (l *RateLimiter) CheckAndIncrement(ctx context.Context, clientID string, hourlyLimit, monthlyLimit int) (Result, error) {
	r, err := l.CheckAll(ctx, clientID, hourlyLimit, monthlyLimit)
	if err != nil {
		return Result{}, err
	}
	return Result{Allowed: r.Allowed, Remaining: r.Remaining, ResetAt: r.ResetAt}, nil
}

// CurrentUsage returns the raw hourly and monthly counters.
// Deprecated: use GetCurrentUsage which returns a structured Usage value.
func (l *RateLimiter) CurrentUsage(ctx context.Context, clientID string) (hourly, monthly int64, err error) {
	usage, err := l.GetCurrentUsage(ctx, clientID, 0, 0)
	if err != nil {
		return 0, 0, err
	}
	return usage.HourlyUsed, usage.MonthlyUsed, nil
}
