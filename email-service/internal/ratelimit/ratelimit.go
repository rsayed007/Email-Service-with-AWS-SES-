// Package ratelimit provides per-client rate limiting for the email service.
//
// Hourly limits use a Redis sliding-window algorithm (sorted set + Lua) so the
// window always covers the last 60 minutes regardless of clock alignment.
//
// Monthly limits use a fixed calendar-month window (a simple counter with TTL)
// because storing thousands of individual timestamps per client per month would
// be prohibitively expensive in memory.
//
// Redis key format:
//   hourly  → ratelimit:{clientID}:hourly          (sorted set, TTL = 3601 s)
//   monthly → ratelimit:{clientID}:monthly:{YYYYMM} (string counter, TTL to EOM)
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ── Types ─────────────────────────────────────────────────────────────────────

// LimitResult is returned by CheckHourlyLimit, CheckMonthlyLimit and CheckAll.
type LimitResult struct {
	Allowed   bool
	Remaining int64     // requests still permitted in the current window
	ResetAt   time.Time // approximate time the window resets / oldest entry expires
}

// Usage is returned by GetCurrentUsage for dashboard and stats endpoints.
type Usage struct {
	HourlyUsed     int64     `json:"hourly_used"`
	HourlyLimit    int       `json:"hourly_limit"`
	HourlyResetAt  time.Time `json:"hourly_reset_at"`
	MonthlyUsed    int64     `json:"monthly_used"`
	MonthlyLimit   int       `json:"monthly_limit"`
	MonthlyResetAt time.Time `json:"monthly_reset_at"`
}

// RateLimiter performs atomic check-and-increment operations against Redis.
type RateLimiter struct {
	rdb *redis.Client
}

// NewRateLimiter creates a RateLimiter backed by rdb.
func NewRateLimiter(rdb *redis.Client) *RateLimiter {
	return &RateLimiter{rdb: rdb}
}

// ── Lua scripts ───────────────────────────────────────────────────────────────

// hourlyScript implements a sliding-window check-and-increment.
//
// KEYS[1] = sorted-set key
// ARGV[1] = window duration in milliseconds (3_600_000)
// ARGV[2] = limit (int)
// ARGV[3] = current Unix time in milliseconds
// ARGV[4] = unique member ID for this request
//
// Returns: {allowed, remaining, oldest_score_ms}
//   allowed        1 = ok, 0 = limit exceeded
//   remaining      requests left after this one (only meaningful when allowed=1)
//   oldest_score_ms score of oldest entry; used to compute ResetAt when denied
var hourlyScript = redis.NewScript(`
local key      = KEYS[1]
local window   = tonumber(ARGV[1])
local limit    = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local uid      = ARGV[4]
local cutoff   = now - window

redis.call('ZREMRANGEBYSCORE', key, 0, cutoff)
local count = tonumber(redis.call('ZCARD', key))

if count >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local oldest_ms = 0
    if #oldest >= 2 then oldest_ms = tonumber(oldest[2]) end
    return {0, 0, oldest_ms}
end

redis.call('ZADD', key, now, uid)
redis.call('EXPIRE', key, math.ceil(window / 1000) + 1)
return {1, limit - count - 1, 0}
`)

// monthlyScript implements a fixed-window check-and-increment for the current
// calendar month.
//
// KEYS[1] = counter key  (ratelimit:{id}:monthly:{YYYYMM})
// ARGV[1] = limit (int)
// ARGV[2] = TTL in seconds until the key should expire (seconds until EOM + buffer)
//
// Returns: {allowed, remaining}
var monthlyScript = redis.NewScript(`
local key   = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl   = tonumber(ARGV[2])

local count = tonumber(redis.call('GET', key) or '0')
if count >= limit then
    return {0, 0}
end

local new = redis.call('INCR', key)
if new == 1 then
    redis.call('EXPIRE', key, ttl)
end
return {1, limit - new}
`)

// checkAllScript combines hourly (sliding) and monthly (fixed) into a single
// atomic Lua execution so a successful hourly increment is never left dangling
// if the monthly limit happens to be exceeded.
//
// KEYS[1] = hourly sorted-set key
// KEYS[2] = monthly counter key
// ARGV[1] = hourly window ms
// ARGV[2] = hourly limit
// ARGV[3] = monthly limit
// ARGV[4] = now_ms
// ARGV[5] = unique member ID
// ARGV[6] = monthly TTL seconds
//
// Returns: {result_code, hour_remaining, month_remaining, oldest_hour_ms}
//   result_code  1 = allowed, 0 = hourly exceeded, -1 = monthly exceeded
var checkAllScript = redis.NewScript(`
local hkey     = KEYS[1]
local mkey     = KEYS[2]
local hwindow  = tonumber(ARGV[1])
local hlimit   = tonumber(ARGV[2])
local mlimit   = tonumber(ARGV[3])
local now      = tonumber(ARGV[4])
local uid      = ARGV[5]
local mttl     = tonumber(ARGV[6])
local cutoff   = now - hwindow

-- Hourly sliding-window check
redis.call('ZREMRANGEBYSCORE', hkey, 0, cutoff)
local hcount = tonumber(redis.call('ZCARD', hkey))

if hcount >= hlimit then
    local oldest = redis.call('ZRANGE', hkey, 0, 0, 'WITHSCORES')
    local oldest_ms = 0
    if #oldest >= 2 then oldest_ms = tonumber(oldest[2]) end
    return {0, 0, mlimit, oldest_ms}
end

-- Monthly fixed-window check
local mcount = tonumber(redis.call('GET', mkey) or '0')
if mcount >= mlimit then
    return {-1, hlimit - hcount, 0, 0}
end

-- Both ok — commit both increments
redis.call('ZADD', hkey, now, uid)
redis.call('EXPIRE', hkey, math.ceil(hwindow / 1000) + 1)

local new_m = redis.call('INCR', mkey)
if new_m == 1 then
    redis.call('EXPIRE', mkey, mttl)
end

return {1, hlimit - hcount - 1, mlimit - new_m, 0}
`)

// ── Public API ────────────────────────────────────────────────────────────────

// CheckHourlyLimit checks and increments the sliding hourly window for clientID.
// limit is the maximum number of emails the client may send per 60-minute window.
func (l *RateLimiter) CheckHourlyLimit(ctx context.Context, clientID string, limit int) (LimitResult, error) {
	now := time.Now().UTC()
	key := hourlyKey(clientID)
	nowMs := now.UnixMilli()
	windowMs := int64(time.Hour / time.Millisecond)
	uid := uuid.New().String()

	vals, err := hourlyScript.Run(ctx, l.rdb,
		[]string{key},
		windowMs, limit, nowMs, uid,
	).Slice()
	if err != nil {
		return LimitResult{}, fmt.Errorf("CheckHourlyLimit: %w", err)
	}

	allowed := toInt64(vals[0]) == 1
	remaining := toInt64(vals[1])
	oldestMs := toInt64(vals[2])

	resetAt := now.Truncate(time.Hour).Add(time.Hour) // default approximate
	if !allowed && oldestMs > 0 {
		resetAt = time.UnixMilli(oldestMs + windowMs).UTC()
	}

	return LimitResult{Allowed: allowed, Remaining: remaining, ResetAt: resetAt}, nil
}

// CheckMonthlyLimit checks and increments the fixed monthly counter for clientID.
// limit is the maximum number of emails the client may send in the current calendar month.
func (l *RateLimiter) CheckMonthlyLimit(ctx context.Context, clientID string, limit int) (LimitResult, error) {
	now := time.Now().UTC()
	key := monthlyKey(clientID, now)
	ttl := secondsUntilEndOfMonth(now)

	vals, err := monthlyScript.Run(ctx, l.rdb,
		[]string{key},
		limit, ttl,
	).Slice()
	if err != nil {
		return LimitResult{}, fmt.Errorf("CheckMonthlyLimit: %w", err)
	}

	allowed := toInt64(vals[0]) == 1
	remaining := toInt64(vals[1])

	return LimitResult{
		Allowed:   allowed,
		Remaining: remaining,
		ResetAt:   firstOfNextMonth(now),
	}, nil
}

// CheckAll atomically checks both the hourly sliding window and the monthly
// fixed counter. On success both counters are incremented in the same Lua call,
// so a successful hourly increment is never stranded when the monthly limit is hit.
func (l *RateLimiter) CheckAll(ctx context.Context, clientID string, hourlyLimit, monthlyLimit int) (LimitResult, error) {
	now := time.Now().UTC()
	hKey := hourlyKey(clientID)
	mKey := monthlyKey(clientID, now)
	nowMs := now.UnixMilli()
	windowMs := int64(time.Hour / time.Millisecond)
	uid := uuid.New().String()
	mTTL := secondsUntilEndOfMonth(now)

	vals, err := checkAllScript.Run(ctx, l.rdb,
		[]string{hKey, mKey},
		windowMs, hourlyLimit, monthlyLimit, nowMs, uid, mTTL,
	).Slice()
	if err != nil {
		return LimitResult{}, fmt.Errorf("CheckAll: %w", err)
	}

	code := toInt64(vals[0])
	hRemaining := toInt64(vals[1])
	_ = toInt64(vals[2]) // mRemaining (available if callers want it)
	oldestMs := toInt64(vals[3])

	switch code {
	case 1: // allowed
		return LimitResult{
			Allowed:   true,
			Remaining: hRemaining, // hourly is typically the tighter constraint
			ResetAt:   now.Truncate(time.Hour).Add(time.Hour),
		}, nil

	case 0: // hourly exceeded
		resetAt := now.Truncate(time.Hour).Add(time.Hour)
		if oldestMs > 0 {
			resetAt = time.UnixMilli(oldestMs + windowMs).UTC()
		}
		return LimitResult{Allowed: false, Remaining: 0, ResetAt: resetAt}, nil

	default: // -1 = monthly exceeded
		return LimitResult{Allowed: false, Remaining: 0, ResetAt: firstOfNextMonth(now)}, nil
	}
}

// GetCurrentUsage returns the client's current counters without modifying them.
// Pass the client's configured hourly and monthly limits so the Usage struct
// is self-contained for API responses.
func (l *RateLimiter) GetCurrentUsage(ctx context.Context, clientID string, hourlyLimit, monthlyLimit int) (*Usage, error) {
	now := time.Now().UTC()
	hKey := hourlyKey(clientID)
	mKey := monthlyKey(clientID, now)
	windowMs := now.UnixMilli() - int64(time.Hour/time.Millisecond)

	pipe := l.rdb.Pipeline()
	// Count sorted-set members with score > (now - 1h), i.e. within the sliding window.
	hCmd := pipe.ZCount(ctx, hKey, fmt.Sprintf("%d", windowMs+1), "+inf")
	mCmd := pipe.Get(ctx, mKey)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("GetCurrentUsage: %w", err)
	}

	var hUsed, mUsed int64
	if v, err := hCmd.Result(); err == nil {
		hUsed = v
	}
	if v, err := mCmd.Int64(); err == nil {
		mUsed = v
	}

	return &Usage{
		HourlyUsed:     hUsed,
		HourlyLimit:    hourlyLimit,
		HourlyResetAt:  now.Truncate(time.Hour).Add(time.Hour),
		MonthlyUsed:    mUsed,
		MonthlyLimit:   monthlyLimit,
		MonthlyResetAt: firstOfNextMonth(now),
	}, nil
}

// ── Key helpers ───────────────────────────────────────────────────────────────

func hourlyKey(clientID string) string {
	return fmt.Sprintf("ratelimit:%s:hourly", clientID)
}

func monthlyKey(clientID string, t time.Time) string {
	return fmt.Sprintf("ratelimit:%s:monthly:%s", clientID, t.Format("200601"))
}

// ── Time helpers ──────────────────────────────────────────────────────────────

func firstOfNextMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

func secondsUntilEndOfMonth(t time.Time) int64 {
	eom := firstOfNextMonth(t)
	// Add a 5-minute buffer so the key doesn't expire exactly at midnight.
	return int64(eom.Add(5*time.Minute).Sub(t).Seconds())
}

// toInt64 coerces an interface{} returned by a Lua script into int64.
// Redis Lua integers come back as int64; other types are treated as 0.
func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	}
	return 0
}
