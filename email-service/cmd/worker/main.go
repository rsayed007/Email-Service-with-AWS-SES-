package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/redis/go-redis/v9"

	"email-service/internal/config"
	"email-service/internal/delivery"
	"email-service/internal/queue"
	"email-service/internal/repository"
	"email-service/internal/tracking"
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

	// ── Worker ────────────────────────────────────────────────────────────────
	w := &Worker{
		queue:     queue.NewQueue(rdb),
		ses:       delivery.NewSESDelivery(awsCfg, cfg.AWS.SNSTopicARN),
		injector:  tracking.NewInjector(cfg.Tracking.BaseURL),
		logs:      repository.NewEmailLogRepository(db),
		stats:     repository.NewStatsRepository(db),
		blacklist: repository.NewBlacklistRepository(db),
		logger:    logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w.Start(ctx, cfg.Worker.Concurrency)
	logger.Info("worker pool started", "concurrency", cfg.Worker.Concurrency)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("worker pool shutting down")
	cancel()
	w.Wait()
	logger.Info("worker pool stopped")
}

// ── Worker ────────────────────────────────────────────────────────────────────

// Worker manages a pool of goroutines that dequeue and deliver email jobs.
type Worker struct {
	queue     *queue.Queue
	ses       *delivery.SESDelivery
	injector  *tracking.Injector
	logs      *repository.EmailLogRepository
	stats     *repository.StatsRepository
	blacklist *repository.BlacklistRepository
	logger    *slog.Logger
	wg        sync.WaitGroup
}

// Start launches concurrency goroutines and returns immediately.
// Call Wait to block until all goroutines exit after ctx is cancelled.
func (w *Worker) Start(ctx context.Context, concurrency int) {
	for i := 0; i < concurrency; i++ {
		w.wg.Add(1)
		go func(id int) {
			defer w.wg.Done()
			w.run(ctx, id)
		}(i)
	}
}

// Wait blocks until all worker goroutines have exited.
func (w *Worker) Wait() {
	w.wg.Wait()
}

func (w *Worker) run(ctx context.Context, id int) {
	w.logger.Debug("worker started", "worker_id", id)
	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("worker stopped", "worker_id", id)
			return
		default:
		}

		job, err := w.queue.Pop(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				w.logger.Error("queue pop error", "worker_id", id, "error", err)
			}
			continue
		}
		if job == nil {
			continue // pop timeout — no job available
		}

		w.logger.Info("processing job",
			"worker_id", id,
			"job_id", job.ID,
			"client_id", job.ClientID,
			"attempt", job.Attempts+1,
		)

		if err := w.processJob(ctx, job); err != nil {
			w.logger.Error("job failed",
				"worker_id", id,
				"job_id", job.ID,
				"attempt", job.Attempts+1,
				"error", err,
			)

			// Permanent SES errors must not be retried.
			var sesErr delivery.SESError
			if errors.As(err, &sesErr) && !sesErr.IsRetryable() {
				_ = w.logs.UpdateStatus(ctx, job.ID, repository.StatusFailed)
				continue
			}

			// Re-queue or move to DLQ when retry limit is reached.
			if retryErr := w.queue.Retry(ctx, job, err.Error()); retryErr != nil {
				w.logger.Error("retry failed", "worker_id", id, "job_id", job.ID, "error", retryErr)
			}
			if job.Attempts >= queue.MaxRetries {
				w.logger.Warn("job exhausted retries, marked failed",
					"worker_id", id, "job_id", job.ID)
				_ = w.logs.UpdateStatus(ctx, job.ID, repository.StatusFailed)
			}
			continue
		}

		w.logger.Info("job completed", "worker_id", id, "job_id", job.ID)
	}
}

// processJob performs a single end-to-end email delivery:
//  1. Filters blacklisted recipients.
//  2. Injects open-pixel and click-tracking into the HTML body.
//  3. Sends via AWS SES.
//  4. Persists the SES message ID and updates the log status.
//  5. Increments daily sent stats.
func (w *Worker) processJob(ctx context.Context, job *queue.EmailJob) error {
	// 1. Blacklist filter.
	validTo := make([]string, 0, len(job.To))
	for _, addr := range job.To {
		bl, err := w.blacklist.IsBlacklisted(ctx, job.ClientID, addr)
		if err != nil {
			return fmt.Errorf("blacklist check %s: %w", addr, err)
		}
		if !bl {
			validTo = append(validTo, addr)
		}
	}
	if len(validTo) == 0 {
		// All recipients are blacklisted — mark as failed without SES call.
		_ = w.logs.UpdateStatus(ctx, job.ID, repository.StatusFailed)
		return nil
	}

	// 2. Tracking injection.
	htmlBody := job.HTMLBody
	if htmlBody != "" {
		htmlBody = w.injector.InjectClickTracking(htmlBody, job.ID)
		htmlBody = w.injector.InjectOpenPixel(htmlBody, job.ID)
	}

	// 3. SES delivery.
	result, err := w.ses.Send(ctx, &delivery.EmailJob{
		ClientID: job.ClientID,
		LogID:    job.ID,
		From:     job.From,
		To:       validTo,
		ReplyTo:  job.ReplyTo,
		Subject:  job.Subject,
		HTMLBody: htmlBody,
		TextBody: job.TextBody,
	})
	if err != nil {
		return fmt.Errorf("ses.Send: %w", err)
	}

	// 4. Persist result.
	if err := w.logs.SetAWSMessageID(ctx, job.ID, result.MessageID); err != nil {
		w.logger.Warn("set aws message ID failed", "job_id", job.ID, "error", err)
	}
	if err := w.logs.UpdateStatus(ctx, job.ID, repository.StatusSent); err != nil {
		return fmt.Errorf("update log status: %w", err)
	}

	// 5. Daily stats.
	_ = w.stats.IncrementStat(ctx, job.ClientID, time.Now().UTC(), "sent")

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
