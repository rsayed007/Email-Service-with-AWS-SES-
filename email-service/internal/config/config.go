package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config is the top-level application configuration loaded from environment variables.
type Config struct {
	App       AppConfig
	Database  DatabaseConfig
	Redis     RedisConfig
	AWS       AWSConfig
	APIServer ServerConfig
	SMTP      SMTPConfig
	Tracking  TrackingConfig
	Queue     QueueConfig
	Worker    WorkerConfig
	Security  SecurityConfig
}

// AppConfig holds generic application settings.
type AppConfig struct {
	Env      string // APP_ENV: development | staging | production
	LogLevel string // APP_LOG_LEVEL: debug | info | warn | error
}

func (a AppConfig) IsProd() bool  { return a.Env == "production" }
func (a AppConfig) IsDebug() bool { return a.LogLevel == "debug" }

// DatabaseConfig holds MySQL connection settings.
type DatabaseConfig struct {
	Host            string        // MYSQL_HOST
	Port            string        // MYSQL_PORT
	User            string        // MYSQL_USER
	Password        string        // MYSQL_PASSWORD
	Database        string        // MYSQL_DATABASE
	MaxOpenConns    int           // MYSQL_MAX_OPEN_CONNS
	MaxIdleConns    int           // MYSQL_MAX_IDLE_CONNS
	ConnMaxLifetime time.Duration // MYSQL_CONN_MAX_LIFETIME
}

// DSN returns the full MySQL data source name including credentials.
func (c DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&collation=utf8mb4_unicode_ci",
		c.User, c.Password, c.Host, c.Port, c.Database,
	)
}

// SafeDSN returns a DSN with the password redacted, safe for logging.
func (c DatabaseConfig) SafeDSN() string {
	return fmt.Sprintf("%s:***@tcp(%s:%s)/%s", c.User, c.Host, c.Port, c.Database)
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Host     string // REDIS_HOST
	Port     string // REDIS_PORT
	Password string // REDIS_PASSWORD
	DB       int    // REDIS_DB
	PoolSize int    // REDIS_POOL_SIZE
}

// Addr returns the "host:port" address string.
func (c RedisConfig) Addr() string { return c.Host + ":" + c.Port }

// AWSConfig holds AWS credentials and service configuration.
type AWSConfig struct {
	Region              string // AWS_REGION
	AccessKeyID         string // AWS_ACCESS_KEY_ID (optional when using an IAM role)
	SecretAccessKey     string // AWS_SECRET_ACCESS_KEY (optional when using an IAM role)
	SESConfigurationSet string // SES_CONFIGURATION_SET
	SNSTopicARN         string // SNS_TOPIC_ARN
	SNSWebhookSecret    string // SNS_WEBHOOK_SECRET
}

// ServerConfig holds HTTP API server settings.
type ServerConfig struct {
	Port         string        // API_PORT
	ReadTimeout  time.Duration // API_READ_TIMEOUT
	WriteTimeout time.Duration // API_WRITE_TIMEOUT
	IdleTimeout  time.Duration // API_IDLE_TIMEOUT
}

// SMTPConfig holds SMTP proxy server settings.
type SMTPConfig struct {
	Port              string        // SMTP_PORT (default: 587)
	Domain            string        // SMTP_DOMAIN
	TLSCertFile       string        // SMTP_TLS_CERT_FILE — PEM certificate for STARTTLS
	TLSKeyFile        string        // SMTP_TLS_KEY_FILE  — PEM private key for STARTTLS
	MaxMessageBytes   int64         // SMTP_MAX_MESSAGE_BYTES (default: 25 MiB)
	MaxRecipients     int           // SMTP_MAX_RECIPIENTS (default: 50)
	MaxConnections    int           // SMTP_MAX_CONNECTIONS (default: 1000)
	ReadTimeout       time.Duration // SMTP_READ_TIMEOUT (default: 60s)
	WriteTimeout      time.Duration // SMTP_WRITE_TIMEOUT (default: 30s)
	AllowInsecureAuth bool          // SMTP_ALLOW_INSECURE_AUTH — set true in dev when no TLS
}

// TrackingConfig holds email open/click tracking settings.
type TrackingConfig struct {
	BaseURL    string // TRACKING_BASE_URL
	PixelPath  string // TRACKING_PIXEL_PATH
	ClickPath  string // TRACKING_CLICK_PATH
	HMACSecret []byte // TRACKING_HMAC_SECRET (hex-encoded 32 bytes minimum)
}

// QueueConfig holds Redis job queue settings.
type QueueConfig struct {
	MaxRetries int           // QUEUE_RETRY_MAX
	RetryDelay time.Duration // QUEUE_RETRY_DELAY
}

// WorkerConfig holds queue worker process settings.
type WorkerConfig struct {
	Concurrency int // QUEUE_CONCURRENCY
}

// SecurityConfig holds security-related settings.
type SecurityConfig struct {
	BcryptCost int // BCRYPT_COST
}

// Load reads configuration from the environment, loading a .env file first if present.
// Returns a descriptive validation error listing every missing or invalid field.
func Load() (*Config, error) {
	// Best-effort .env load; production environments supply real env vars directly.
	_ = godotenv.Load()

	cfg := &Config{}
	cfg.populate()

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// MustLoad calls Load and panics on error. Intended for use in main() where a
// configuration failure is always fatal.
func MustLoad() *Config {
	cfg, err := Load()
	if err != nil {
		panic("config: " + err.Error())
	}
	return cfg
}

// populate fills cfg from environment variables, applying defaults where appropriate.
func (c *Config) populate() {
	// App
	c.App.Env = getEnv("APP_ENV", "development")
	c.App.LogLevel = getEnv("APP_LOG_LEVEL", "info")

	// Database
	c.Database.Host = os.Getenv("MYSQL_HOST")
	c.Database.Port = getEnv("MYSQL_PORT", "3306")
	c.Database.User = os.Getenv("MYSQL_USER")
	c.Database.Password = os.Getenv("MYSQL_PASSWORD")
	c.Database.Database = os.Getenv("MYSQL_DATABASE")
	c.Database.MaxOpenConns = getEnvInt("MYSQL_MAX_OPEN_CONNS", 25)
	c.Database.MaxIdleConns = getEnvInt("MYSQL_MAX_IDLE_CONNS", 10)
	c.Database.ConnMaxLifetime = getEnvDuration("MYSQL_CONN_MAX_LIFETIME", 5*time.Minute)

	// Redis
	c.Redis.Host = getEnv("REDIS_HOST", "localhost")
	c.Redis.Port = getEnv("REDIS_PORT", "6379")
	c.Redis.Password = os.Getenv("REDIS_PASSWORD")
	c.Redis.DB = getEnvInt("REDIS_DB", 0)
	c.Redis.PoolSize = getEnvInt("REDIS_POOL_SIZE", 10)

	// AWS
	c.AWS.Region = os.Getenv("AWS_REGION")
	c.AWS.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	c.AWS.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	c.AWS.SESConfigurationSet = os.Getenv("SES_CONFIGURATION_SET")
	c.AWS.SNSTopicARN = os.Getenv("SNS_TOPIC_ARN")
	c.AWS.SNSWebhookSecret = os.Getenv("SNS_WEBHOOK_SECRET")

	// API Server
	c.APIServer.Port = getEnv("API_PORT", "8080")
	c.APIServer.ReadTimeout = getEnvDuration("API_READ_TIMEOUT", 30*time.Second)
	c.APIServer.WriteTimeout = getEnvDuration("API_WRITE_TIMEOUT", 30*time.Second)
	c.APIServer.IdleTimeout = getEnvDuration("API_IDLE_TIMEOUT", 60*time.Second)

	// SMTP Server
	c.SMTP.Port = getEnv("SMTP_PORT", "587")
	c.SMTP.Domain = getEnv("SMTP_DOMAIN", "localhost")
	c.SMTP.TLSCertFile = os.Getenv("SMTP_TLS_CERT_FILE")
	c.SMTP.TLSKeyFile = os.Getenv("SMTP_TLS_KEY_FILE")
	c.SMTP.MaxMessageBytes = int64(getEnvInt("SMTP_MAX_MESSAGE_BYTES", 25*1024*1024))
	c.SMTP.MaxRecipients = getEnvInt("SMTP_MAX_RECIPIENTS", 50)
	c.SMTP.MaxConnections = getEnvInt("SMTP_MAX_CONNECTIONS", 1000)
	c.SMTP.ReadTimeout = getEnvDuration("SMTP_READ_TIMEOUT", 60*time.Second)
	c.SMTP.WriteTimeout = getEnvDuration("SMTP_WRITE_TIMEOUT", 30*time.Second)
	c.SMTP.AllowInsecureAuth = getEnvBool("SMTP_ALLOW_INSECURE_AUTH", c.SMTP.TLSCertFile == "")

	// Tracking
	c.Tracking.BaseURL = strings.TrimRight(os.Getenv("TRACKING_BASE_URL"), "/")
	c.Tracking.PixelPath = getEnv("TRACKING_PIXEL_PATH", "/t/open")
	c.Tracking.ClickPath = getEnv("TRACKING_CLICK_PATH", "/t/click")
	c.Tracking.HMACSecret = parseHMACSecret(os.Getenv("TRACKING_HMAC_SECRET"))

	// Queue
	c.Queue.MaxRetries = getEnvInt("QUEUE_RETRY_MAX", 3)
	c.Queue.RetryDelay = getEnvDuration("QUEUE_RETRY_DELAY", 5*time.Second)

	// Worker
	c.Worker.Concurrency = getEnvInt("QUEUE_CONCURRENCY", 10)

	// Security
	c.Security.BcryptCost = getEnvInt("BCRYPT_COST", 12)
}

// validate collects every configuration error and returns them joined.
func (c *Config) validate() error {
	var errs []error

	add := func(msg string) { errs = append(errs, errors.New(msg)) }

	// Database — all four fields are required
	if c.Database.Host == "" {
		add("MYSQL_HOST is required")
	}
	if c.Database.User == "" {
		add("MYSQL_USER is required")
	}
	if c.Database.Password == "" {
		add("MYSQL_PASSWORD is required")
	}
	if c.Database.Database == "" {
		add("MYSQL_DATABASE is required")
	}
	if c.Database.MaxOpenConns < c.Database.MaxIdleConns {
		add("MYSQL_MAX_OPEN_CONNS must be >= MYSQL_MAX_IDLE_CONNS")
	}

	// Redis
	if c.Redis.Host == "" {
		add("REDIS_HOST is required")
	}

	// AWS
	if c.AWS.Region == "" {
		add("AWS_REGION is required")
	}

	// Tracking
	if c.Tracking.BaseURL == "" {
		add("TRACKING_BASE_URL is required")
	}
	if len(c.Tracking.HMACSecret) < 16 {
		add("TRACKING_HMAC_SECRET must be at least 16 bytes (32 hex chars)")
	}

	// Constraints
	if c.Security.BcryptCost < 4 || c.Security.BcryptCost > 31 {
		add("BCRYPT_COST must be between 4 and 31")
	}
	if c.Worker.Concurrency < 1 {
		add("QUEUE_CONCURRENCY must be >= 1")
	}
	if c.Queue.MaxRetries < 0 {
		add("QUEUE_RETRY_MAX must be >= 0")
	}

	return errors.Join(errs...)
}

// ── env helpers ───────────────────────────────────────────────────────────────

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func getEnvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

// parseHMACSecret tries hex-decoding first; falls back to raw bytes.
func parseHMACSecret(s string) []byte {
	if s == "" {
		return nil
	}
	if b, err := hex.DecodeString(s); err == nil {
		return b
	}
	return []byte(s)
}
