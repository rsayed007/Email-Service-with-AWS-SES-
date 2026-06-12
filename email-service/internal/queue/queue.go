package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	emailQueueKey = "queue:emails"
	deadQueueKey  = "queue:emails:dead"
	popTimeout    = 5 * time.Second

	// MaxRetries is the number of delivery attempts before a job is moved to
	// the dead-letter queue. Workers export this so they can react accordingly.
	MaxRetries = 3
)

// EmailJob is the Redis-serialised representation of a single send request.
type EmailJob struct {
	ID          string    `json:"id"`           // email_logs.id
	ClientID    string    `json:"client_id"`
	From        string    `json:"from"`
	To          []string  `json:"to"`
	Subject     string    `json:"subject"`
	HTMLBody    string    `json:"html_body"`
	TextBody    string    `json:"text_body"`
	ReplyTo     string    `json:"reply_to,omitempty"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error,omitempty"`
}

// Queue wraps a Redis client to provide a simple FIFO email queue with a
// separate dead-letter queue for permanently failed jobs.
type Queue struct {
	rdb *redis.Client
}

// NewQueue creates a Queue backed by rdb.
func NewQueue(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb}
}

// Push serialises job and prepends it to the email queue (LPUSH).
func (q *Queue) Push(ctx context.Context, job *EmailJob) error {
	job.EnqueuedAt = time.Now().UTC()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue push marshal: %w", err)
	}
	if err := q.rdb.LPush(ctx, emailQueueKey, data).Err(); err != nil {
		return fmt.Errorf("queue push: %w", err)
	}
	return nil
}

// Enqueue is an alias for Push kept for backward compatibility.
func (q *Queue) Enqueue(ctx context.Context, job *EmailJob) error {
	return q.Push(ctx, job)
}

// Pop blocks for up to popTimeout (5 s) waiting for a job. Returns nil, nil
// on timeout so the caller can loop without error.
func (q *Queue) Pop(ctx context.Context) (*EmailJob, error) {
	res, err := q.rdb.BRPop(ctx, popTimeout, emailQueueKey).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue pop: %w", err)
	}
	var job EmailJob
	if err := json.Unmarshal([]byte(res[1]), &job); err != nil {
		return nil, fmt.Errorf("queue pop unmarshal: %w", err)
	}
	return &job, nil
}

// Retry increments job.Attempts and records reason in job.LastError.
// If the new attempt count is below MaxRetries the job is re-queued;
// otherwise it is moved to the dead-letter queue.
func (q *Queue) Retry(ctx context.Context, job *EmailJob, reason string) error {
	job.Attempts++
	job.LastError = reason
	if job.Attempts < MaxRetries {
		return q.push(ctx, emailQueueKey, job)
	}
	return q.push(ctx, deadQueueKey, job)
}

// push is the internal serialise-and-RPUSH helper (puts job at the tail so
// retries are processed after newer jobs).
func (q *Queue) push(ctx context.Context, key string, job *EmailJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue push marshal: %w", err)
	}
	if err := q.rdb.RPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("queue push %s: %w", key, err)
	}
	return nil
}

// Len returns the number of jobs waiting in the active queue.
func (q *Queue) Len(ctx context.Context) (int64, error) {
	n, err := q.rdb.LLen(ctx, emailQueueKey).Result()
	if err != nil {
		return 0, fmt.Errorf("queue len: %w", err)
	}
	return n, nil
}

// DeadLen returns the number of jobs in the dead-letter queue.
func (q *Queue) DeadLen(ctx context.Context) (int64, error) {
	n, err := q.rdb.LLen(ctx, deadQueueKey).Result()
	if err != nil {
		return 0, fmt.Errorf("dead queue len: %w", err)
	}
	return n, nil
}
