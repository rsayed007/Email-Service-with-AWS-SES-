package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
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
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"email-service/internal/auth"
	"email-service/internal/queue"
	"email-service/internal/ratelimit"
	"email-service/internal/repository"
)

func main() {
	_ = godotenv.Load()

	db, err := repository.NewDB(repository.Config{
		Host:            mustEnv("MYSQL_HOST"),
		Port:            getEnv("MYSQL_PORT", "3306"),
		User:            mustEnv("MYSQL_USER"),
		Password:        mustEnv("MYSQL_PASSWORD"),
		Database:        mustEnv("MYSQL_DATABASE"),
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	})
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", mustEnv("REDIS_HOST"), getEnv("REDIS_PORT", "6379")),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})
	defer rdb.Close()

	clientRepo := repository.NewClientRepository(db)
	emailLogRepo := repository.NewEmailLogRepository(db)
	emailQueue := queue.NewQueue(rdb)
	smtpAuth := auth.NewSMTPAuthenticator(clientRepo)
	limiter := ratelimit.NewLimiter(rdb)

	domain := getEnv("SMTP_DOMAIN", "mail.example.com")
	port := getEnv("SMTP_PORT", "2525")

	be := &smtpBackend{
		smtpAuth:   smtpAuth,
		emailLogs:  emailLogRepo,
		emailQueue: emailQueue,
		limiter:    limiter,
	}

	s := smtp.NewServer(be)
	s.Addr = ":" + port
	s.Domain = domain
	s.ReadTimeout = 30 * time.Second
	s.WriteTimeout = 30 * time.Second
	s.MaxMessageBytes = 10 * 1024 * 1024 // 10 MB
	s.MaxRecipients = 50
	s.AllowInsecureAuth = true // set to false in production with TLS

	go func() {
		log.Printf("SMTP proxy listening on :%s (domain: %s)", port, domain)
		if err := s.ListenAndServe(); err != nil {
			log.Fatalf("smtp server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("SMTP server shutting down...")
	if err := s.Close(); err != nil {
		log.Printf("smtp close: %v", err)
	}
	log.Println("SMTP server stopped")
}

// ── Backend ───────────────────────────────────────────────────────────────────

type smtpBackend struct {
	smtpAuth   *auth.SMTPAuthenticator
	emailLogs  *repository.EmailLogRepository
	emailQueue *queue.Queue
	limiter    *ratelimit.Limiter
}

func (b *smtpBackend) NewSession(_ *smtp.Conn) (smtp.Session, error) {
	return &smtpSession{
		backend: b,
	}, nil
}

// ── Session ───────────────────────────────────────────────────────────────────

type smtpSession struct {
	backend *smtpBackend
	client  *repository.Client
	from    string
	to      []string
}

// AuthPlain is called for both AUTH PLAIN and AUTH LOGIN by go-smtp.
func (s *smtpSession) AuthPlain(username, password string) error {
	client, err := s.backend.smtpAuth.Authenticate(context.Background(), username, password)
	if err != nil {
		return fmt.Errorf("authentication failed")
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

	result, err := s.backend.limiter.CheckAndIncrement(ctx, s.client.ID, s.client.HourlyLimit, s.client.MonthlyLimit)
	if err != nil || !result.Allowed {
		return fmt.Errorf("rate limit exceeded")
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

func (s *smtpSession) Logout() error {
	return nil
}

// ── RFC 2822 message parser ───────────────────────────────────────────────────

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
			partCT := part.Header.Get("Content-Type")
			partMedia, _, _ := mime.ParseMediaType(partCT)
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
	b, _ := io.ReadAll(quotedprintable.NewReader(r))
	if len(b) == 0 {
		b, _ = io.ReadAll(r)
	}
	return string(b)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
