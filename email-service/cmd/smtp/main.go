package main

import (
	"bytes"
	"context"
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
	emailQueue := queue.NewQueue(rdb)
	authenticator := auth.New(clientRepo, rdb, cfg.Security.BcryptCost)
	limiter := ratelimit.NewRateLimiter(rdb)

	// ── SMTP server ───────────────────────────────────────────────────────────
	be := &smtpBackend{
		cfg:           cfg.SMTP,
		authenticator: authenticator,
		emailLogs:     emailLogRepo,
		emailQueue:    emailQueue,
		limiter:       limiter,
	}

	s := smtp.NewServer(be)
	s.Addr = ":" + cfg.SMTP.Port
	s.Domain = cfg.SMTP.Domain
	s.ReadTimeout = cfg.SMTP.ReadTimeout
	s.WriteTimeout = cfg.SMTP.WriteTimeout
	s.MaxMessageBytes = cfg.SMTP.MaxMessageBytes
	s.MaxRecipients = cfg.SMTP.MaxRecipients
	s.AllowInsecureAuth = cfg.SMTP.AllowInsecureAuth

	go func() {
		logger.Info("SMTP server starting", "port", cfg.SMTP.Port, "domain", cfg.SMTP.Domain)
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
	authenticator *auth.Authenticator
	emailLogs     *repository.EmailLogRepository
	emailQueue    *queue.Queue
	limiter       *ratelimit.RateLimiter
}

func (b *smtpBackend) NewSession(_ *smtp.Conn) (smtp.Session, error) {
	return &smtpSession{backend: b}, nil
}

// ── Session ───────────────────────────────────────────────────────────────────

type smtpSession struct {
	backend *smtpBackend
	client  *repository.Client
	from    string
	to      []string
}

// AuthPlain is called by go-smtp for both AUTH PLAIN and AUTH LOGIN.
func (s *smtpSession) AuthPlain(username, password string) error {
	client, err := s.backend.authenticator.ValidateSmtpCredentials(
		context.Background(), username, password)
	if err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}
	s.client = client
	return nil
}

func (s *smtpSession) Mail(from string, _ *smtp.MailOptions) error {
	if s.client == nil {
		return fmt.Errorf("authentication required")
	}
	s.from = from
	return nil
}

func (s *smtpSession) Rcpt(to string, _ *smtp.RcptOptions) error {
	if s.client == nil {
		return fmt.Errorf("authentication required")
	}
	s.to = append(s.to, to)
	return nil
}

func (s *smtpSession) Data(r io.Reader) error {
	if s.client == nil {
		return fmt.Errorf("authentication required")
	}
	ctx := context.Background()

	// Atomic check of both hourly (sliding window) and monthly limits.
	rl, err := s.backend.limiter.CheckAll(ctx, s.client.ID,
		s.client.HourlyLimit, s.client.MonthlyLimit)
	if err != nil {
		return fmt.Errorf("rate limit check failed")
	}
	if !rl.Allowed {
		return fmt.Errorf("rate limit exceeded, retry after %s", rl.ResetAt.Format(time.RFC1123))
	}

	rawMsg, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read message: %w", err)
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
		return fmt.Errorf("create email log: %w", err)
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
	if err := s.backend.emailQueue.Enqueue(ctx, job); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}
	return nil
}

func (s *smtpSession) Reset() {
	s.from = ""
	s.to = nil
}

func (s *smtpSession) Logout() error { return nil }

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
