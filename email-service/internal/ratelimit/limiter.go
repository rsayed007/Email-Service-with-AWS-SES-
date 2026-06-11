package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	rdb *redis.Client
}

func NewLimiter(rdb *redis.Client) *Limiter {
	return &Limiter{rdb: rdb}
}

type Result struct {
	Allowed   bool
	Remaining int64
	ResetAt   time.Time
}

// CheckAndIncrement checks whether a client is within both hourly and monthly
// limits. On success it atomically increments both counters.
func (l *Limiter) CheckAndIncrement(ctx context.Context, clientID string, hourlyLimit, monthlyLimit int) (Result, error) {
	now := time.Now().UTC()
	hourKey := fmt.Sprintf("rl:h:%s:%s", clientID, now.Format("2006010215"))
	monthKey := fmt.Sprintf("rl:m:%s:%s", clientID, now.Format("200601"))

	// Check current counts before incrementing.
	pipe := l.rdb.Pipeline()
	hourGet := pipe.Get(ctx, hourKey)
	monthGet := pipe.Get(ctx, monthKey)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return Result{}, fmt.Errorf("rate limit check: %w", err)
	}

	hourCount := int64(0)
	monthCount := int64(0)
	if v, err := hourGet.Int64(); err == nil {
		hourCount = v
	}
	if v, err := monthGet.Int64(); err == nil {
		monthCount = v
	}

	if int(hourCount) >= hourlyLimit {
		return Result{
			Allowed:   false,
			Remaining: 0,
			ResetAt:   now.Truncate(time.Hour).Add(time.Hour),
		}, nil
	}
	if int(monthCount) >= monthlyLimit {
		nextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		return Result{Allowed: false, Remaining: 0, ResetAt: nextMonth}, nil
	}

	// Atomically increment.
	pipe2 := l.rdb.Pipeline()
	hourInc := pipe2.Incr(ctx, hourKey)
	pipe2.Expire(ctx, hourKey, 2*time.Hour)
	pipe2.Incr(ctx, monthKey)
	pipe2.Expire(ctx, monthKey, 32*24*time.Hour)
	if _, err := pipe2.Exec(ctx); err != nil {
		return Result{}, fmt.Errorf("rate limit increment: %w", err)
	}

	newHourCount := hourInc.Val()
	hourRemaining := int64(hourlyLimit) - newHourCount
	if hourRemaining < 0 {
		hourRemaining = 0
	}

	return Result{
		Allowed:   true,
		Remaining: hourRemaining,
		ResetAt:   now.Truncate(time.Hour).Add(time.Hour),
	}, nil
}

// CurrentUsage returns current hourly and monthly counters without modifying them.
func (l *Limiter) CurrentUsage(ctx context.Context, clientID string) (hourly, monthly int64, err error) {
	now := time.Now().UTC()
	hourKey := fmt.Sprintf("rl:h:%s:%s", clientID, now.Format("2006010215"))
	monthKey := fmt.Sprintf("rl:m:%s:%s", clientID, now.Format("200601"))

	pipe := l.rdb.Pipeline()
	hg := pipe.Get(ctx, hourKey)
	mg := pipe.Get(ctx, monthKey)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, 0, fmt.Errorf("current usage: %w", err)
	}
	if v, e := hg.Int64(); e == nil {
		hourly = v
	}
	if v, e := mg.Int64(); e == nil {
		monthly = v
	}
	return hourly, monthly, nil
}
