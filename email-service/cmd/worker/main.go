package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	awscfg "github.com/aws/aws-sdk-go-v2/config"

	"email-service/internal/delivery"
	"email-service/internal/queue"
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

	awsCfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion(mustEnv("AWS_REGION")),
	)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}

	emailLogRepo := repository.NewEmailLogRepository(db)
	statsRepo := repository.NewStatsRepository(db)
	blacklistRepo := repository.NewBlacklistRepository(db)
	sesClient := delivery.NewSESClient(awsCfg, os.Getenv("SES_CONFIGURATION_SET"))
	emailQueue := queue.NewQueue(rdb)

	concurrency := 10
	maxRetries := 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, emailQueue, sesClient, emailLogRepo, statsRepo, blacklistRepo, maxRetries)
		}(i)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("worker shutting down...")
	cancel()
	wg.Wait()
	log.Println("worker stopped")
}

func runWorker(
	ctx context.Context,
	id int,
	q *queue.Queue,
	ses *delivery.SESClient,
	logs *repository.EmailLogRepository,
	stats *repository.StatsRepository,
	blacklist *repository.BlacklistRepository,
	maxRetries int,
) {
	log.Printf("worker %d started", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d stopping", id)
			return
		default:
		}

		job, err := q.Dequeue(ctx, 5*time.Second)
		if err != nil {
			log.Printf("worker %d dequeue error: %v", id, err)
			continue
		}
		if job == nil {
			continue // timeout, no job available
		}

		if err := processJob(ctx, job, ses, logs, stats, blacklist); err != nil {
			log.Printf("worker %d job %s failed (attempt %d): %v", id, job.ID, job.Attempts+1, err)
			if job.Attempts < maxRetries {
				if reqErr := q.Requeue(ctx, job); reqErr != nil {
					log.Printf("worker %d requeue error: %v", id, reqErr)
				}
			} else {
				log.Printf("worker %d job %s exhausted retries, marking failed", id, job.ID)
				_ = logs.UpdateStatus(ctx, job.ID, repository.StatusFailed)
			}
		}
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
	// Filter out blacklisted recipients.
	validTo := make([]string, 0, len(job.To))
	for _, addr := range job.To {
		bl, err := blacklist.IsBlacklisted(ctx, job.ClientID, addr)
		if err != nil || bl {
			log.Printf("job %s: skipping blacklisted recipient %s", job.ID, addr)
			continue
		}
		validTo = append(validTo, addr)
	}
	if len(validTo) == 0 {
		_ = logs.UpdateStatus(ctx, job.ID, repository.StatusFailed)
		return nil
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
		return fmt.Errorf("ses send: %w", err)
	}

	if err := logs.SetAWSMessageID(ctx, job.ID, result.MessageID); err != nil {
		log.Printf("set aws message id: %v", err)
	}
	if err := logs.UpdateStatus(ctx, job.ID, repository.StatusSent); err != nil {
		log.Printf("update status sent: %v", err)
	}
	_ = stats.IncrementStat(ctx, job.ClientID, time.Now().UTC(), "sent")

	return nil
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
