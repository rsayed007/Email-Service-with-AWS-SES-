package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"log/slog"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/emersion/go-smtp"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"email-service/internal/auth"
	"email-service/internal/config"
	"email-service/internal/queue"
	"email-service/internal/ratelimit"
	"email-service/internal/repository"
	"email-service/pkg/database"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	logger := newLogger(cfg.App.LogLevel)

	// ── Database ──────────────────────────────────────────────────────────────
	db, err := repository.NewDB(cfg.Database)
	if err != nil {
		logger.Error("database connect failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := database.RunMigrations(db.DB, cfg.Database); err != nil {
		logger.Error("migrations failed", "error", err)
		os.Exit(1)
	}

	// ── Redis ─────────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})
	defer rdb.Close()

	// ── Dependencies ──────────────────────────────────────────────────────────
	clientRepo := repository.NewClientRepository(db)
	emailLogRepo := repository.NewEmailLogRepository(db)
	blacklistRepo := repository.NewBlacklistRepository(db)
	emailQueue := queue.NewQueue(rdb)
	authenticator := auth.New(clientRepo, rdb, cfg.Security.BcryptCost)
	limiter := ratelimit.NewRateLimiter(rdb)

	// ── SMTP backend ──────────────────────────────────────────────────────────
	be := &smtpBackend{
		cfg:           cfg.SMTP,
		logger:        logger,
		authenticator: authenticator,
		emailLogs:     emailLogRepo,
		blacklist:     blacklistRepo,
		emailQueue:    emailQueue,
		limiter:       limiter,
		maxConns:      int64(cfg.SMTP.MaxConnections),
	}

	// ── Server ────────────────────────────────────────────────────────────────
	s := smtp.NewServer(be)
	s.Addr = ":" + cfg.SMTP.Port
	s.Domain = cfg.SMTP.Domain
	s.ReadTimeout = cfg.SMTP.ReadTimeout
	s.WriteTimeout = cfg.SMTP.WriteTimeout
	s.MaxMessageBytes = cfg.SMTP.MaxMessageBytes
	s.MaxRecipients = cfg.SMTP.MaxRecipients
	s.AllowInsecureAuth = cfg.SMTP.AllowInsecureAuth

	// STARTTLS — enabled when cert and key are provided.
	if cfg.SMTP.TLSCertFile != "" && cfg.SMTP.TLSKeyFile != "" {
		tlsCert, err := tls.LoadX509KeyPair(cfg.SMTP.TLSCertFile, cfg.SMTP.TLSKeyFile)
		if err != nil {
			logger.Error("load TLS certificate failed", "error", err)
			os.Exit(1)
		}
		s.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			MinVersion:   tls.VersionTLS12,
		}
		s.AllowInsecureAuth = false // require STARTTLS before AUTH when TLS is configured
		logger.Info("STARTTLS enabled", "cert", cfg.SMTP.TLSCertFile)
	}

	go func() {
		logger.Info("SMTP server starting",
			"port", cfg.SMTP.Port,
			"domain", cfg.SMTP.Domain,
			"tls", s.TLSConfig != nil,
		)
		if err := s.ListenAndServe(); err != nil {
			logger.Error("SMTP server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("SMTP server shutting down")
	if err := s.Close(); err != nil {
		logger.Error("SMTP close error", "error", err)
	}
	logger.Info("SMTP server stopped")
}

// ── Backend ───────────────────────────────────────────────────────────────────

type smtpBackend struct {
	cfg           config.SMTPConfig
	logger        *slog.Logger
	authenticator *auth.Authenticator
	emailLogs     *repository.EmailLogRepository
	blacklist     *repository.BlacklistRepository
	emailQueue    *queue.Queue
	limiter       *ratelimit.RateLimiter

	maxConns  int64 // configured limit
	connCount int64 // current active connections (atomic)
}

func (b *smtpBackend) NewSession(c *smtp.Conn) (smtp.Session, error) {
	if b.maxConns > 0 {
		cur := atomic.AddInt64(&b.connCount, 1)
		if cur > b.maxConns {
			atomic.AddInt64(&b.connCount, -1)
			return nil, &smtp.SMTPError{Code: 421, EnhancedCode: smtp.EnhancedCode{4, 4, 5},
				Message: "too many connections, try later"}
		}
	}
	return &smtpSession{backend: b}, nil
}

// ── Session ───────────────────────────────────────────────────────────────────

type smtpSession struct {
	backend *smtpBackend
	client  *repository.Client
	from    string
	to      []string
}

// AuthPlain is called by go-smtp for AUTH PLAIN and AUTH LOGIN.
func (s *smtpSession) AuthPlain(username, password string) error {
	client, err := s.backend.authenticator.ValidateSmtpCredentials(
		context.Background(), username, password)
	if err != nil {
		return &smtp.SMTPError{Code: 535, EnhancedCode: smtp.EnhancedCode{5, 7, 8},
			Message: "authentication failed"}
	}
	s.client = client
	return nil
}

// Mail stores the envelope sender and verifies it is not blacklisted.
func (s *smtpSession) Mail(from string, _ *smtp.MailOptions) error {
	if s.client == nil {
		return &smtp.SMTPError{Code: 530, EnhancedCode: smtp.EnhancedCode{5, 7, 0},
			Message: "authentication required"}
	}
	bl, err := s.backend.blacklist.IsBlacklisted(context.Background(), s.client.ID, from)
	if err == nil && bl {
		return &smtp.SMTPError{Code: 553, EnhancedCode: smtp.EnhancedCode{5, 1, 3},
			Message: "sender address rejected"}
	}
	s.from = from
	return nil
}

// Rcpt appends a recipient, checking blacklist and per-message recipient cap.
func (s *smtpSession) Rcpt(to string, _ *smtp.RcptOptions) error {
	if s.client == nil {
		return &smtp.SMTPError{Code: 530, EnhancedCode: smtp.EnhancedCode{5, 7, 0},
			Message: "authentication required"}
	}
	if len(s.to) >= s.backend.cfg.MaxRecipients {
		return &smtp.SMTPError{Code: 452, EnhancedCode: smtp.EnhancedCode{4, 5, 3},
			Message: fmt.Sprintf("max %d recipients per message", s.backend.cfg.MaxRecipients)}
	}
	bl, err := s.backend.blacklist.IsBlacklisted(context.Background(), s.client.ID, to)
	if err == nil && bl {
		return &smtp.SMTPError{Code: 550, EnhancedCode: smtp.EnhancedCode{5, 1, 1},
			Message: "recipient address blacklisted"}
	}
	s.to = append(s.to, to)
	return nil
}

// Data reads the message body, enforces rate limits (returning 451 for hourly
// and 452 for monthly quota), records an email_log entry, and enqueues the job.
func (s *smtpSession) Data(r io.Reader) error {
	if s.client == nil {
		return &smtp.SMTPError{Code: 530, EnhancedCode: smtp.EnhancedCode{5, 7, 0},
			Message: "authentication required"}
	}
	ctx := context.Background()

	// Atomic dual-limit check. Distinguish hourly (4xx transient) from monthly
	// (452 storage-exceeded) based on when the window resets.
	rl, err := s.backend.limiter.CheckAll(ctx,
		s.client.ID, s.client.HourlyLimit, s.client.MonthlyLimit)
	if err != nil {
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0},
			Message: "internal error checking rate limits"}
	}
	if !rl.Allowed {
		if rl.ResetAt.Sub(time.Now()) > 2*time.Hour {
			// Reset is far away → monthly quota exhausted.
			return &smtp.SMTPError{Code: 452, EnhancedCode: smtp.EnhancedCode{4, 2, 2},
				Message: "monthly email quota exceeded"}
		}
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 4, 5},
			Message: fmt.Sprintf("hourly rate limit exceeded, retry after %s",
				rl.ResetAt.UTC().Format(time.RFC1123))}
	}

	rawMsg, err := io.ReadAll(io.LimitReader(r, s.backend.cfg.MaxMessageBytes+1))
	if err != nil {
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0},
			Message: "error reading message data"}
	}
	if int64(len(rawMsg)) > s.backend.cfg.MaxMessageBytes {
		return &smtp.SMTPError{Code: 552, EnhancedCode: smtp.EnhancedCode{5, 3, 4},
			Message: "message size exceeds limit"}
	}

	subject, htmlBody, textBody := parseRawMessage(rawMsg)

	logID := uuid.New().String()
	toAddr := ""
	if len(s.to) > 0 {
		toAddr = s.to[0]
	}

	emailLog := &repository.EmailLog{
		ID:        logID,
		ClientID:  s.client.ID,
		FromEmail: s.from,
		ToEmail:   toAddr,
		Subject:   subject,
		Status:    repository.StatusQueued,
	}
	if err := s.backend.emailLogs.Create(ctx, emailLog); err != nil {
		s.backend.logger.Error("create email log failed",
			"client_id", s.client.ID, "error", err)
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0},
			Message: "failed to record message"}
	}

	job := &queue.EmailJob{
		ID:       logID,
		ClientID: s.client.ID,
		From:     s.from,
		To:       s.to,
		Subject:  subject,
		HTMLBody: htmlBody,
		TextBody: textBody,
	}
	if err := s.backend.emailQueue.Push(ctx, job); err != nil {
		s.backend.logger.Error("queue push failed",
			"client_id", s.client.ID, "log_id", logID, "error", err)
		return &smtp.SMTPError{Code: 451, EnhancedCode: smtp.EnhancedCode{4, 3, 0},
			Message: "failed to queue message"}
	}

	s.backend.logger.Info("message accepted",
		"log_id", logID,
		"client_id", s.client.ID,
		"from", s.from,
		"recipients", len(s.to),
	)
	return nil
}

func (s *smtpSession) Reset() {
	s.from = ""
	s.to = nil
}

func (s *smtpSession) Logout() error {
	atomic.AddInt64(&s.backend.connCount, -1)
	return nil
}

// ── RFC 2822 parser ───────────────────────────────────────────────────────────

func parseRawMessage(raw []byte) (subject, htmlBody, textBody string) {
	msg, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return "", "", string(raw)
	}
	subject = msg.Header.Get("Subject")

	ct := msg.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(ct)
	if err != nil {
		b, _ := io.ReadAll(msg.Body)
		return subject, "", string(b)
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(msg.Body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}
			partMedia, _, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
			body := readPart(part)
			switch partMedia {
			case "text/html":
				htmlBody = body
			case "text/plain":
				textBody = body
			}
		}
		return subject, htmlBody, textBody
	}

	body := readPart(msg.Body)
	if mediaType == "text/html" {
		return subject, body, ""
	}
	return subject, "", body
}

func readPart(r io.Reader) string {
	b, err := io.ReadAll(quotedprintable.NewReader(r))
	if err != nil || len(b) == 0 {
		b, _ = io.ReadAll(r)
	}
	return string(b)
}

// newLogger is kept here to avoid importing a shared package for a one-liner.
func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

