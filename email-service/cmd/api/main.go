package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	awscfg "github.com/aws/aws-sdk-go-v2/config"

	"email-service/internal/auth"
	"email-service/internal/config"
	"email-service/internal/delivery"
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

	// ── AWS ───────────────────────────────────────────────────────────────────
	awsCfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion(cfg.AWS.Region),
	)
	if err != nil {
		logger.Error("aws config failed", "error", err)
		os.Exit(1)
	}

	// ── Repositories ──────────────────────────────────────────────────────────
	clientRepo := repository.NewClientRepository(db)
	emailLogRepo := repository.NewEmailLogRepository(db)
	statsRepo := repository.NewStatsRepository(db)
	blacklistRepo := repository.NewBlacklistRepository(db)

	// ── Services ──────────────────────────────────────────────────────────────
	authenticator := auth.New(clientRepo, rdb, cfg.Security.BcryptCost)
	limiter := ratelimit.NewRateLimiter(rdb)
	emailQueue := queue.NewQueue(rdb)
	_ = delivery.NewSESClient(awsCfg, cfg.AWS.SESConfigurationSet) // sending done by worker
	tracker := tracking.NewTracker(
		cfg.Tracking.HMACSecret,
		cfg.Tracking.BaseURL,
		cfg.Tracking.PixelPath,
		cfg.Tracking.ClickPath,
	)
	snsHandler := webhook.NewSNSHandler(emailLogRepo, statsRepo, blacklistRepo)

	// ── Router ────────────────────────────────────────────────────────────────
	if cfg.App.IsProd() {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// SNS events — AWS signs its own payloads; no auth middleware here.
	r.POST("/webhooks/sns", snsHandler.Handle)

	// Open-tracking pixel.
	r.GET(cfg.Tracking.PixelPath, func(c *gin.Context) {
		tok, err := tracker.Verify(c.Query("t"))
		if err == nil {
			_ = emailLogRepo.UpdateStatus(c.Request.Context(), tok.LogID, repository.StatusOpened)
			_ = statsRepo.IncrementStat(c.Request.Context(), tok.ClientID, time.Now().UTC(), "opened")
		}
		c.Data(http.StatusOK, "image/gif", transparentGIF)
	})

	// Click-redirect.
	r.GET(cfg.Tracking.ClickPath, func(c *gin.Context) {
		tok, err := tracker.Verify(c.Query("t"))
		if err != nil || tok.URL == "" {
			c.Status(http.StatusBadRequest)
			return
		}
		_ = emailLogRepo.UpdateStatus(c.Request.Context(), tok.LogID, repository.StatusClicked)
		_ = statsRepo.IncrementStat(c.Request.Context(), tok.ClientID, time.Now().UTC(), "clicked")
		c.Redirect(http.StatusFound, tok.URL)
	})

	// ── Authenticated REST endpoints ──────────────────────────────────────────
	api := r.Group("/api/v1", middleware.APIKeyMiddleware(authenticator))

	// POST /api/v1/emails — enqueue a send request
	api.POST("/emails", func(c *gin.Context) {
		client, err := auth.ClientFromContext(c)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}

		var req struct {
			From     string   `json:"from"      binding:"required,email"`
			To       []string `json:"to"        binding:"required,min=1"`
			Subject  string   `json:"subject"   binding:"required"`
			HTMLBody string   `json:"html_body"`
			TextBody string   `json:"text_body"`
			ReplyTo  string   `json:"reply_to"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Atomic dual rate-limit check (hourly sliding window + monthly counter).
		rl, err := limiter.CheckAll(c.Request.Context(), client.ID, client.HourlyLimit, client.MonthlyLimit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "rate limit check failed"})
			return
		}
		if !rl.Allowed {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":     "rate limit exceeded",
				"reset_at":  rl.ResetAt,
				"remaining": rl.Remaining,
			})
			return
		}

		// Blacklist check on the primary recipient.
		if bl, _ := blacklistRepo.IsBlacklisted(c.Request.Context(), client.ID, req.To[0]); bl {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "recipient is blacklisted"})
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
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create email record"})
			return
		}

		job := &queue.EmailJob{
			ID:       logID,
			ClientID: client.ID,
			From:     req.From,
			To:       req.To,
			Subject:  req.Subject,
			HTMLBody: req.HTMLBody,
			TextBody: req.TextBody,
			ReplyTo:  req.ReplyTo,
		}
		if err := emailQueue.Enqueue(c.Request.Context(), job); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue email"})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{
			"message_id": logID,
			"status":     repository.StatusQueued,
			"remaining":  rl.Remaining,
		})
	})

	// GET /api/v1/emails
	api.GET("/emails", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		logs, err := emailLogRepo.List(c.Request.Context(), repository.LogFilter{
			ClientID: client.ID,
			Status:   c.Query("status"),
			Limit:    50,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list emails"})
			return
		}
		c.JSON(http.StatusOK, logs)
	})

	// GET /api/v1/emails/:id
	api.GET("/emails/:id", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		l, err := emailLogRepo.GetByID(c.Request.Context(), c.Param("id"))
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		if l.ClientID != client.ID {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}
		c.JSON(http.StatusOK, l)
	})

	// GET /api/v1/stats
	api.GET("/stats", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		now := time.Now().UTC()
		from := now.AddDate(0, 0, -30)

		daily, err := statsRepo.GetRange(c.Request.Context(), client.ID, from, now)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get daily stats"})
			return
		}
		summary, err := statsRepo.GetSummary(c.Request.Context(), client.ID, from, now)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get summary"})
			return
		}
		usage, err := limiter.GetCurrentUsage(c.Request.Context(), client.ID, client.HourlyLimit, client.MonthlyLimit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get usage"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"summary": summary,
			"daily":   daily,
			"usage":   usage,
		})
	})

	// GET /api/v1/blacklist
	api.GET("/blacklist", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		entries, err := blacklistRepo.List(c.Request.Context(), client.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list blacklist"})
			return
		}
		c.JSON(http.StatusOK, entries)
	})

	// DELETE /api/v1/blacklist/:email
	api.DELETE("/blacklist/:email", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		if err := blacklistRepo.Remove(c.Request.Context(), client.ID, c.Param("email")); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to remove from blacklist"})
			return
		}
		c.Status(http.StatusNoContent)
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

// transparentGIF is a 1×1 GIF returned by the open-tracking pixel endpoint.
var transparentGIF = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00,
	0x80, 0x00, 0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21,
	0xf9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00,
	0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3b,
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
