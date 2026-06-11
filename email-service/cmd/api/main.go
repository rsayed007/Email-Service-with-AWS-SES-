package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	awscfg "github.com/aws/aws-sdk-go-v2/config"

	"email-service/internal/auth"
	"email-service/internal/delivery"
	"email-service/internal/queue"
	"email-service/internal/ratelimit"
	"email-service/internal/repository"
	"email-service/internal/tracking"
	"email-service/internal/webhook"
)

func main() {
	_ = godotenv.Load()

	db, err := repository.NewDB(repository.Config{
		Host:            mustEnv("MYSQL_HOST"),
		Port:            getEnv("MYSQL_PORT", "3306"),
		User:            mustEnv("MYSQL_USER"),
		Password:        mustEnv("MYSQL_PASSWORD"),
		Database:        mustEnv("MYSQL_DATABASE"),
		MaxOpenConns:    25,
		MaxIdleConns:    10,
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

	awsCfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion(mustEnv("AWS_REGION")),
	)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}

	// Repositories
	clientRepo := repository.NewClientRepository(db)
	emailLogRepo := repository.NewEmailLogRepository(db)
	statsRepo := repository.NewStatsRepository(db)
	blacklistRepo := repository.NewBlacklistRepository(db)

	// Services
	apiAuth := auth.NewAPIKeyAuthenticator(clientRepo)
	limiter := ratelimit.NewLimiter(rdb)
	emailQueue := queue.NewQueue(rdb)
	sesClient := delivery.NewSESClient(awsCfg, os.Getenv("SES_CONFIGURATION_SET"))
	tracker := tracking.NewTracker(
		[]byte(mustEnv("TRACKING_HMAC_SECRET")),
		mustEnv("TRACKING_BASE_URL"),
		getEnv("TRACKING_PIXEL_PATH", "/t/open"),
		getEnv("TRACKING_CLICK_PATH", "/t/click"),
	)
	snsHandler := webhook.NewSNSHandler(emailLogRepo, statsRepo, blacklistRepo)
	_ = sesClient // used via queue worker; kept for direct-send option

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// ── SNS webhook (no auth required — SNS signs its own messages) ──────────
	r.POST("/webhooks/sns", snsHandler.Handle)

	// ── Tracking endpoints ────────────────────────────────────────────────────
	r.GET("/t/open", func(c *gin.Context) {
		tokenStr := c.Query("t")
		tok, err := tracker.Verify(tokenStr)
		if err == nil {
			_ = emailLogRepo.UpdateStatus(c.Request.Context(), tok.LogID, repository.StatusOpened)
			_ = statsRepo.IncrementStat(c.Request.Context(), tok.ClientID, time.Now().UTC(), "opened")
		}
		// Return 1x1 transparent GIF
		c.Data(200, "image/gif", transparentGIF)
	})

	r.GET("/t/click", func(c *gin.Context) {
		tokenStr := c.Query("t")
		tok, err := tracker.Verify(tokenStr)
		if err != nil || tok.URL == "" {
			c.Status(400)
			return
		}
		_ = emailLogRepo.UpdateStatus(c.Request.Context(), tok.LogID, repository.StatusClicked)
		_ = statsRepo.IncrementStat(c.Request.Context(), tok.ClientID, time.Now().UTC(), "clicked")
		c.Redirect(http.StatusFound, tok.URL)
	})

	// ── Authenticated API ─────────────────────────────────────────────────────
	api := r.Group("/api/v1", apiAuth.Middleware())

	// Send email
	api.POST("/emails", func(c *gin.Context) {
		client, err := auth.ClientFromContext(c)
		if err != nil {
			c.JSON(500, gin.H{"error": "internal error"})
			return
		}

		var req struct {
			From     string   `json:"from"     binding:"required,email"`
			To       []string `json:"to"       binding:"required,min=1"`
			Subject  string   `json:"subject"  binding:"required"`
			HTMLBody string   `json:"html_body"`
			TextBody string   `json:"text_body"`
			ReplyTo  string   `json:"reply_to"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		// Rate limit check
		result, err := limiter.CheckAndIncrement(c.Request.Context(), client.ID, client.HourlyLimit, client.MonthlyLimit)
		if err != nil {
			c.JSON(500, gin.H{"error": "rate limit check failed"})
			return
		}
		if !result.Allowed {
			c.JSON(429, gin.H{
				"error":    "rate limit exceeded",
				"reset_at": result.ResetAt,
			})
			return
		}

		// Blacklist check (only first recipient for simplicity; worker checks all)
		if len(req.To) > 0 {
			blacklisted, _ := blacklistRepo.IsBlacklisted(c.Request.Context(), client.ID, req.To[0])
			if blacklisted {
				c.JSON(422, gin.H{"error": "recipient is blacklisted"})
				return
			}
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
			c.JSON(500, gin.H{"error": "failed to create email log"})
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
			c.JSON(500, gin.H{"error": "failed to enqueue email"})
			return
		}

		c.JSON(202, gin.H{
			"message_id": logID,
			"status":     "queued",
		})
	})

	// List emails
	api.GET("/emails", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		logs, err := emailLogRepo.List(c.Request.Context(), repository.LogFilter{
			ClientID: client.ID,
			Status:   c.Query("status"),
			Limit:    50,
		})
		if err != nil {
			c.JSON(500, gin.H{"error": "failed to list emails"})
			return
		}
		c.JSON(200, logs)
	})

	// Get single email
	api.GET("/emails/:id", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		log, err := emailLogRepo.GetByID(c.Request.Context(), c.Param("id"))
		if err != nil {
			c.JSON(404, gin.H{"error": "not found"})
			return
		}
		if log.ClientID != client.ID {
			c.JSON(403, gin.H{"error": "forbidden"})
			return
		}
		c.JSON(200, log)
	})

	// Stats
	api.GET("/stats", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		from := time.Now().UTC().AddDate(0, 0, -30)
		to := time.Now().UTC()
		stats, err := statsRepo.GetRange(c.Request.Context(), client.ID, from, to)
		if err != nil {
			c.JSON(500, gin.H{"error": "failed to get stats"})
			return
		}
		hourly, monthly, _ := limiter.CurrentUsage(c.Request.Context(), client.ID)
		c.JSON(200, gin.H{
			"daily":          stats,
			"hourly_usage":   hourly,
			"monthly_usage":  monthly,
			"hourly_limit":   client.HourlyLimit,
			"monthly_limit":  client.MonthlyLimit,
		})
	})

	// Blacklist
	api.GET("/blacklist", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		entries, err := blacklistRepo.List(c.Request.Context(), client.ID)
		if err != nil {
			c.JSON(500, gin.H{"error": "failed to list blacklist"})
			return
		}
		c.JSON(200, entries)
	})

	api.DELETE("/blacklist/:email", func(c *gin.Context) {
		client, _ := auth.ClientFromContext(c)
		if err := blacklistRepo.Remove(c.Request.Context(), client.ID, c.Param("email")); err != nil {
			c.JSON(500, gin.H{"error": "failed to remove from blacklist"})
			return
		}
		c.Status(204)
	})

	port := getEnv("API_PORT", "8080")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("API server listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down API server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server shutdown: %v", err)
	}
	log.Println("API server stopped")
}

// 1x1 transparent GIF
var transparentGIF = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00,
	0x80, 0x00, 0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21,
	0xf9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00,
	0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3b,
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
