package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"email-service/internal/api"
	"email-service/internal/auth"
	"email-service/internal/config"
	"email-service/internal/middleware"
	"email-service/internal/queue"
	"email-service/internal/ratelimit"
	"email-service/internal/repository"
	"email-service/internal/tracking"
	"email-service/internal/webhook"
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
	logger.Info("database migrations up to date", "dsn", cfg.Database.SafeDSN())

	// ── Redis ─────────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Error("redis ping failed", "error", err)
		os.Exit(1)
	}

	// ── Repositories ──────────────────────────────────────────────────────────
	clientRepo    := repository.NewClientRepository(db)
	emailLogRepo  := repository.NewEmailLogRepository(db)
	statsRepo     := repository.NewStatsRepository(db)
	blacklistRepo := repository.NewBlacklistRepository(db)

	// ── Services ──────────────────────────────────────────────────────────────
	authenticator := auth.New(clientRepo, rdb, cfg.Security.BcryptCost)
	limiter       := ratelimit.NewRateLimiter(rdb)
	emailQueue    := queue.NewQueue(rdb)
	trackHandlers := tracking.NewHandlers(emailLogRepo, statsRepo, logger)
	snsHandler    := webhook.NewSNSHandler(emailLogRepo, statsRepo, blacklistRepo, logger)

	// ── Router ────────────────────────────────────────────────────────────────
	if cfg.App.IsProd() {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	// ── Public routes ─────────────────────────────────────────────────────────

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"status": "ok"}})
	})

	// SNS webhook — signature verification runs before the handler.
	r.POST("/webhooks/ses", webhook.SNSVerifyMiddleware(), snsHandler.Handle)

	// ── Tracking (no auth — must respond immediately) ──────────────────────────

	r.GET("/o/:logId", trackHandlers.HandleOpen)
	r.GET("/c/:logId", trackHandlers.HandleClick)

	// ── Client API (/v1/*) ────────────────────────────────────────────────────

	v1 := r.Group("/v1", middleware.APIKeyMiddleware(authenticator))

	// POST /v1/email/send
	v1.POST("/email/send", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)

		var req struct {
			To      []string `json:"to"       binding:"required,min=1"`
			Subject string   `json:"subject"  binding:"required"`
			HTML    string   `json:"html"`
			Text    string   `json:"text"`
			From    string   `json:"from"`
			ReplyTo string   `json:"reply_to"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}
		if req.From == "" {
			req.From = "noreply@" + cfg.SMTP.Domain
		}

		rl, err := limiter.CheckAll(c.Request.Context(), client.ID, client.HourlyLimit, client.MonthlyLimit)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "rate limit check failed")
			return
		}
		if !rl.Allowed {
			api.Err(c, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED",
				"rate limit exceeded, retry after "+rl.ResetAt.UTC().Format(time.RFC1123))
			return
		}

		if bl, _ := blacklistRepo.IsBlacklisted(c.Request.Context(), client.ID, req.To[0]); bl {
			api.Err(c, http.StatusUnprocessableEntity, "RECIPIENT_BLACKLISTED", "recipient is blacklisted")
			return
		}

		logID := uuid.New().String()
		emailLog := &repository.EmailLog{
			ID:        logID,
			ClientID:  client.ID,
			FromEmail: req.From,
			ToEmail:   req.To[0],
			Subject:   req.Subject,
			Status:    repository.StatusQueued,
		}
		if err := emailLogRepo.Create(c.Request.Context(), emailLog); err != nil {
			logger.Error("create email log failed", "client_id", client.ID, "error", err)
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create email record")
			return
		}

		job := &queue.EmailJob{
			ID:       logID,
			ClientID: client.ID,
			From:     req.From,
			To:       req.To,
			Subject:  req.Subject,
			HTMLBody: req.HTML,
			TextBody: req.Text,
			ReplyTo:  req.ReplyTo,
		}
		if err := emailQueue.Push(c.Request.Context(), job); err != nil {
			logger.Error("queue push failed", "client_id", client.ID, "log_id", logID, "error", err)
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to enqueue email")
			return
		}

		api.Accepted(c, gin.H{
			"message_id": logID,
			"log_id":     logID,
			"status":     repository.StatusQueued,
		})
	})

	// GET /v1/stats/overview?from=2024-01-01&to=2024-01-31
	v1.GET("/stats/overview", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)

		now := time.Now().UTC()
		from, to := now.AddDate(0, 0, -30), now

		if s := c.Query("from"); s != "" {
			if t, err := time.Parse("2006-01-02", s); err == nil {
				from = t.UTC()
			}
		}
		if s := c.Query("to"); s != "" {
			if t, err := time.Parse("2006-01-02", s); err == nil {
				// inclusive: include all events on the to-date
				to = t.UTC().Add(24*time.Hour - time.Nanosecond)
			}
		}

		daily, err := statsRepo.GetRange(c.Request.Context(), client.ID, from, to)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve daily stats")
			return
		}
		summary, err := statsRepo.GetSummary(c.Request.Context(), client.ID, from, to)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve summary")
			return
		}

		api.OK(c, gin.H{
			"sent":            summary.Sent,
			"delivered":       summary.Delivered,
			"opened":          summary.Opened,
			"clicked":         summary.Clicked,
			"bounced":         summary.Bounced,
			"complained":      summary.Complained,
			"open_rate":       roundRate(float64(summary.Opened), float64(summary.Delivered)),
			"bounce_rate":     roundRate(float64(summary.Bounced), float64(summary.Sent)),
			"daily_breakdown": daily,
		})
	})

	// GET /v1/emails?page=1&limit=50&status=delivered
	v1.GET("/emails", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)

		page  := parseIntQuery(c, "page", 1, 1, 10000)
		limit := parseIntQuery(c, "limit", 50, 1, 200)

		f := repository.LogFilter{
			ClientID: client.ID,
			Status:   c.Query("status"),
			Limit:    limit,
			Offset:   (page - 1) * limit,
		}

		logs, err := emailLogRepo.List(c.Request.Context(), f)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list emails")
			return
		}
		// Count uses the same filters but without paging.
		total, err := emailLogRepo.Count(c.Request.Context(), repository.LogFilter{
			ClientID: client.ID,
			Status:   f.Status,
		})
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to count emails")
			return
		}

		api.Page(c, logs, page, limit, total)
	})

	// GET /v1/quota
	v1.GET("/quota", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)

		usage, err := limiter.GetCurrentUsage(c.Request.Context(), client.ID, client.HourlyLimit, client.MonthlyLimit)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve quota")
			return
		}
		api.OK(c, usage)
	})

	// ── Admin API (/admin/*) ──────────────────────────────────────────────────

	adm := r.Group("/admin", middleware.AdminKeyMiddleware(cfg.Security.AdminAPIKey))

	// POST /admin/clients — provision a new tenant
	adm.POST("/clients", func(c *gin.Context) {
		var req struct {
			Name         string `json:"name"          binding:"required"`
			SMTPUsername string `json:"smtp_username" binding:"required"`
			HourlyLimit  int    `json:"hourly_limit"`
			MonthlyLimit int    `json:"monthly_limit"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}
		if req.HourlyLimit <= 0 {
			req.HourlyLimit = 100
		}
		if req.MonthlyLimit <= 0 {
			req.MonthlyLimit = 10000
		}

		apiKey, err := auth.GenerateAPIKey()
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate api key")
			return
		}

		// Generate a random SMTP password — shown once, stored only as bcrypt hash.
		smtpPassBytes := make([]byte, 16)
		if _, err := rand.Read(smtpPassBytes); err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate password")
			return
		}
		smtpPassword := base64.RawURLEncoding.EncodeToString(smtpPassBytes)

		passwordHash, err := auth.HashPassword(smtpPassword, cfg.Security.BcryptCost)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to hash password")
			return
		}

		client := &repository.Client{
			ID:               uuid.New().String(),
			Name:             req.Name,
			SMTPUsername:     req.SMTPUsername,
			SMTPPasswordHash: passwordHash,
			APIKey:           apiKey,
			HourlyLimit:      req.HourlyLimit,
			MonthlyLimit:     req.MonthlyLimit,
			IsActive:         true,
		}
		if err := clientRepo.Create(c.Request.Context(), client); err != nil {
			if errors.Is(err, repository.ErrDuplicate) {
				api.Err(c, http.StatusConflict, "DUPLICATE_CLIENT",
					"smtp_username or api_key already exists")
				return
			}
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create client")
			return
		}

		api.Created(c, gin.H{
			"client":        client,
			"smtp_password": smtpPassword, // plaintext — only returned here
			"smtp_host":     cfg.SMTP.Domain,
			"smtp_port":     cfg.SMTP.Port,
		})
	})

	// GET /admin/clients
	adm.GET("/clients", func(c *gin.Context) {
		clients, err := clientRepo.List(c.Request.Context())
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list clients")
			return
		}
		api.OK(c, clients)
	})

	// PUT /admin/clients/:id/status — activate or deactivate a tenant
	adm.PUT("/clients/:id/status", func(c *gin.Context) {
		var req struct {
			IsActive bool `json:"is_active"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
			return
		}

		client, err := clientRepo.GetByID(c.Request.Context(), c.Param("id"))
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				api.Err(c, http.StatusNotFound, "NOT_FOUND", "client not found")
				return
			}
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve client")
			return
		}

		client.IsActive = req.IsActive
		if err := clientRepo.Update(c.Request.Context(), client); err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to update client status")
			return
		}

		// Evict cached credentials so the new status takes effect immediately.
		_ = authenticator.InvalidateCache(c.Request.Context(), client)

		api.OK(c, client)
	})

	// GET /admin/clients/:id/stats — last-30-days stats for a specific tenant
	adm.GET("/clients/:id/stats", func(c *gin.Context) {
		clientID := c.Param("id")
		if _, err := clientRepo.GetByID(c.Request.Context(), clientID); err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				api.Err(c, http.StatusNotFound, "NOT_FOUND", "client not found")
				return
			}
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve client")
			return
		}

		now  := time.Now().UTC()
		from := now.AddDate(0, 0, -30)

		daily, err := statsRepo.GetRange(c.Request.Context(), clientID, from, now)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve daily stats")
			return
		}
		summary, err := statsRepo.GetSummary(c.Request.Context(), clientID, from, now)
		if err != nil {
			api.Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve summary")
			return
		}

		api.OK(c, gin.H{
			"sent":            summary.Sent,
			"delivered":       summary.Delivered,
			"opened":          summary.Opened,
			"clicked":         summary.Clicked,
			"bounced":         summary.Bounced,
			"complained":      summary.Complained,
			"open_rate":       roundRate(float64(summary.Opened), float64(summary.Delivered)),
			"bounce_rate":     roundRate(float64(summary.Bounced), float64(summary.Sent)),
			"daily_breakdown": daily,
		})
	})

	// ── HTTP server with graceful shutdown ────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.APIServer.Port,
		Handler:      r,
		ReadTimeout:  cfg.APIServer.ReadTimeout,
		WriteTimeout: cfg.APIServer.WriteTimeout,
		IdleTimeout:  cfg.APIServer.IdleTimeout,
	}

	go func() {
		logger.Info("API server starting", "port", cfg.APIServer.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("API server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("API server shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
	logger.Info("API server stopped")
}

// ── helpers ───────────────────────────────────────────────────────────────────

// roundRate returns numerator/denominator rounded to 4 decimal places,
// or 0 when denominator is zero.
func roundRate(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return math.Round(numerator/denominator*10000) / 10000
}

// parseIntQuery reads a query param as int, clamping to [min, max] and falling
// back to defaultVal when absent or unparseable.
func parseIntQuery(c *gin.Context, key string, defaultVal, min, max int) int {
	v, err := strconv.Atoi(c.Query(key))
	if err != nil || v < min {
		return defaultVal
	}
	if v > max {
		return max
	}
	return v
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
