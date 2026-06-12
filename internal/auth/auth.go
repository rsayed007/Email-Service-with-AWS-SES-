// Package auth handles client authentication for both the REST API (API keys)
// and the SMTP proxy (username + password). Successful DB lookups are cached
// in Redis for 5 minutes to reduce load; bcrypt comparisons always execute.
package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"

	"email-service/internal/repository"
)

const (
	apiKeyCachePrefix = "auth:client:apikey:"
	smtpCachePrefix   = "auth:client:smtp:"
	defaultCacheTTL   = 5 * time.Minute
	apiKeyPrefix      = "em_live_"
	apiKeyRandBytes   = 24 // produces 32 base64url chars → 40-char key total
)

// Authenticator validates client credentials and caches results in Redis.
type Authenticator struct {
	clients    *repository.ClientRepository
	rdb        *redis.Client
	bcryptCost int
	cacheTTL   time.Duration
}

// New creates an Authenticator.  bcryptCost should come from config.Security.BcryptCost.
func New(clients *repository.ClientRepository, rdb *redis.Client, bcryptCost int) *Authenticator {
	return &Authenticator{
		clients:    clients,
		rdb:        rdb,
		bcryptCost: bcryptCost,
		cacheTTL:   defaultCacheTTL,
	}
}

// ValidateAPIKey returns the active Client for apiKey. The result is cached in
// Redis so repeated requests within the TTL skip the database entirely.
func (a *Authenticator) ValidateAPIKey(ctx context.Context, apiKey string) (client *repository.Client, err error) {
	if apiKey == "" {
		return nil, errors.New("api key is empty")
	}

	cacheKey := apiKeyCachePrefix + apiKey

	if client, err = a.getFromCache(ctx, cacheKey); err == nil {
		return client, nil
	}

	client, err = a.clients.GetByAPIKey(ctx, apiKey)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, errors.New("invalid api key")
		}
		return nil, fmt.Errorf("ValidateAPIKey: %w", err)
	}

	a.writeToCache(ctx, cacheKey, client)
	return client, nil
}

// ValidateSmtpCredentials looks up the client by username (using the cache to
// avoid repeated DB round-trips) and always runs bcrypt to verify the password.
// Negative results (wrong password, unknown user) are never cached.
func (a *Authenticator) ValidateSmtpCredentials(ctx context.Context, username, password string) (client *repository.Client, err error) {
	if username == "" || password == "" {
		return nil, errors.New("username and password are required")
	}

	cacheKey := smtpCachePrefix + username

	if client, err = a.getFromCache(ctx, cacheKey); err != nil {
		// Cache miss — hit the DB.
		client, err = a.clients.GetBySMTPUsername(ctx, username)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				return nil, errors.New("invalid smtp credentials")
			}
			return nil, fmt.Errorf("ValidateSmtpCredentials: %w", err)
		}
		a.writeToCache(ctx, cacheKey, client)
	}

	// bcrypt must run on every call — never cache password-verification outcomes.
	if err = bcrypt.CompareHashAndPassword([]byte(client.SMTPPasswordHash), []byte(password)); err != nil {
		return nil, errors.New("invalid smtp credentials")
	}

	return client, nil
}

// InvalidateCache removes the cached entries for a client — call this after
// rotating the client's API key or SMTP password, or when deactivating the account.
func (a *Authenticator) InvalidateCache(ctx context.Context, client *repository.Client) error {
	keys := []string{
		apiKeyCachePrefix + client.APIKey,
		smtpCachePrefix + client.SMTPUsername,
	}
	if err := a.rdb.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("InvalidateCache: %w", err)
	}
	return nil
}

// ── Package-level helpers ─────────────────────────────────────────────────────

// GenerateAPIKey creates a cryptographically random key with the "em_live_" prefix.
// Format: em_live_<32 base64url chars>  (40 chars total)
func GenerateAPIKey() (string, error) {
	b := make([]byte, apiKeyRandBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("GenerateAPIKey: %w", err)
	}
	return apiKeyPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// HashPassword returns a bcrypt hash of password.
// cost should come from config.Security.BcryptCost; values outside [4,31] fall
// back to bcrypt.DefaultCost.
func HashPassword(password string, cost int) (string, error) {
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		cost = bcrypt.DefaultCost
	}
	h, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return "", fmt.Errorf("HashPassword: %w", err)
	}
	return string(h), nil
}

// ── Redis cache helpers ───────────────────────────────────────────────────────

// clientSnapshot is a local DTO used exclusively for Redis serialisation.
// Unlike repository.Client it includes SMTPPasswordHash (tagged json:"-" on
// the model) so SMTP auth can compare passwords against the cached record.
type clientSnapshot struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	SMTPUsername     string    `json:"smtp_username"`
	SMTPPasswordHash string    `json:"smtp_password_hash"` // bcrypt hash, not plaintext
	APIKey           string    `json:"api_key"`
	HourlyLimit      int       `json:"hourly_limit"`
	MonthlyLimit     int       `json:"monthly_limit"`
	IsActive         bool      `json:"is_active"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func toSnapshot(c *repository.Client) clientSnapshot {
	return clientSnapshot{
		ID:               c.ID,
		Name:             c.Name,
		SMTPUsername:     c.SMTPUsername,
		SMTPPasswordHash: c.SMTPPasswordHash,
		APIKey:           c.APIKey,
		HourlyLimit:      c.HourlyLimit,
		MonthlyLimit:     c.MonthlyLimit,
		IsActive:         c.IsActive,
		CreatedAt:        c.CreatedAt,
		UpdatedAt:        c.UpdatedAt,
	}
}

func fromSnapshot(s clientSnapshot) *repository.Client {
	return &repository.Client{
		ID:               s.ID,
		Name:             s.Name,
		SMTPUsername:     s.SMTPUsername,
		SMTPPasswordHash: s.SMTPPasswordHash,
		APIKey:           s.APIKey,
		HourlyLimit:      s.HourlyLimit,
		MonthlyLimit:     s.MonthlyLimit,
		IsActive:         s.IsActive,
		CreatedAt:        s.CreatedAt,
		UpdatedAt:        s.UpdatedAt,
	}
}

func (a *Authenticator) getFromCache(ctx context.Context, key string) (*repository.Client, error) {
	data, err := a.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err // redis.Nil on cache miss
	}
	var snap clientSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal cached client: %w", err)
	}
	return fromSnapshot(snap), nil
}

func (a *Authenticator) writeToCache(ctx context.Context, key string, client *repository.Client) {
	data, err := json.Marshal(toSnapshot(client))
	if err != nil {
		return // shouldn't happen; best-effort only
	}
	// Ignore errors: a miss just means an extra DB lookup next time.
	_ = a.rdb.Set(ctx, key, data, a.cacheTTL).Err()
}
