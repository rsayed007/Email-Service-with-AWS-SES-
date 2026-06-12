package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	awscfg "github.com/aws/aws-sdk-go-v2/config"

	"email-service/internal/config"
	"email-service/internal/delivery"
	"email-service/internal/queue"
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

	// ── AWS ───────────────────────────────────────────────────────────────────
	awsCfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion(cfg.AWS.Region),
	)
	if err != nil {
		logger.Error("aws config failed", "error", err)
		os.Exit(1)
	}

	// ── Dependencies ──────────────────────────────────────────────────────────
	emailLogRepo := repository.NewEmailLogRepository(db)
	statsRepo := repository.NewStatsRepository(db)
	blacklistRepo := repository.NewBlacklistRepository(db)
	sesClient := delivery.NewSESClient(awsCfg, cfg.AWS.SESConfigurationSet)
	emailQueue := queue.NewQueue(rdb)

	// ── Worker pool ───────────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < cfg.Worker.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorker(ctx, id, logger, cfg.Queue,
				emailQueue, sesClient, emailLogRepo, statsRepo, blacklistRepo)
		}(i)
	}
	logger.Info("worker pool started", "concurrency", cfg.Worker.Concurrency)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("worker pool shutting down")
	cancel()
	wg.Wait()
	logger.Info("worker pool stopped")
}

func runWorker(
	ctx context.Context,
	id int,
	logger *slog.Logger,
	qcfg config.QueueConfig,
	q *queue.Queue,
	ses *delivery.SESClient,
	logs *repository.EmailLogRepository,
	stats *repository.StatsRepository,
	blacklist *repository.BlacklistRepository,
) {
	logger.Debug("worker started", "worker_id", id)
	for {
		select {
		case <-ctx.Done():
			logger.Debug("worker stopped", "worker_id", id)
			return
		default:
		}

		job, err := q.Dequeue(ctx, 5*time.Second)
		if err != nil {
			logger.Error("dequeue error", "worker_id", id, "error", err)
			continue
		}
		if job == nil {
			continue // timeout, no job available
		}

		logger.Info("processing job", "worker_id", id, "job_id", job.ID, "attempt", job.Attempts+1)

		if err := processJob(ctx, job, ses, logs, stats, blacklist); err != nil {
			logger.Error("job failed", "worker_id", id, "job_id", job.ID,
				"attempt", job.Attempts+1, "error", err)

			if job.Attempts < qcfg.MaxRetries {
				time.Sleep(qcfg.RetryDelay)
				if reqErr := q.Requeue(ctx, job); reqErr != nil {
					logger.Error("requeue error", "worker_id", id, "job_id", job.ID, "error", reqErr)
				}
			} else {
				logger.Warn("job exhausted retries, marking failed",
					"worker_id", id, "job_id", job.ID)
				_ = logs.UpdateStatus(ctx, job.ID, repository.StatusFailed)
			}
			continue
		}

		logger.Info("job completed", "worker_id", id, "job_id", job.ID)
	}
}

func processJob(
	ctx context.Context,
	job *queue.EmailJob,
	ses *delivery.SESClient,
	logs *repository.EmailLogRepository,
	stats *repository.StatsRepository,
	blacklist *repository.BlacklistRepository,
) error {
	// Filter blacklisted recipients before calling SES.
	validTo := make([]string, 0, len(job.To))
	for _, addr := range job.To {
		bl, err := blacklist.IsBlacklisted(ctx, job.ClientID, addr)
		if err != nil {
			return fmt.Errorf("blacklist check for %s: %w", addr, err)
		}
		if !bl {
			validTo = append(validTo, addr)
		}
	}

	if len(validTo) == 0 {
		return logs.UpdateStatus(ctx, job.ID, repository.StatusFailed)
	}

	req := delivery.SendRequest{
		From:     job.From,
		To:       validTo,
		Subject:  job.Subject,
		HTMLBody: job.HTMLBody,
		TextBody: job.TextBody,
		Tags: map[string]string{
			"client_id": job.ClientID,
			"log_id":    job.ID,
		},
	}
	if job.ReplyTo != "" {
		req.ReplyTo = []string{job.ReplyTo}
	}

	result, err := ses.Send(ctx, req)
	if err != nil {
		return fmt.Errorf("ses.Send: %w", err)
	}

	if err := logs.SetAWSMessageID(ctx, job.ID, result.MessageID); err != nil {
		// Non-fatal: continue so the status update still happens.
		_ = err
	}
	if err := logs.UpdateStatus(ctx, job.ID, repository.StatusSent); err != nil {
		return fmt.Errorf("update status sent: %w", err)
	}
	_ = stats.IncrementStat(ctx, job.ClientID, time.Now().UTC(), "sent")

	return nil
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
