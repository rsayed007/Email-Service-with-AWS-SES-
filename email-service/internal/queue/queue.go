package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	emailQueueKey  = "queue:emails"
	processingKey  = "queue:processing"
)

type EmailJob struct {
	ID        string    `json:"id"`          // email_log id
	ClientID  string    `json:"client_id"`
	From      string    `json:"from"`
	To        []string  `json:"to"`
	Subject   string    `json:"subject"`
	HTMLBody  string    `json:"html_body"`
	TextBody  string    `json:"text_body"`
	ReplyTo   string    `json:"reply_to,omitempty"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Attempts  int       `json:"attempts"`
}

type Queue struct {
	rdb *redis.Client
}

func NewQueue(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb}
}

func (q *Queue) Enqueue(ctx context.Context, job *EmailJob) error {
	job.EnqueuedAt = time.Now().UTC()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	if err := q.rdb.LPush(ctx, emailQueueKey, data).Err(); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}
	return nil
}

// Dequeue blocks up to timeout waiting for a job.
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*EmailJob, error) {
	res, err := q.rdb.BRPop(ctx, timeout, emailQueueKey).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dequeue job: %w", err)
	}
	var job EmailJob
	if err := json.Unmarshal([]byte(res[1]), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

// Requeue puts a failed job back at the tail of the queue after incrementing attempts.
func (q *Queue) Requeue(ctx context.Context, job *EmailJob) error {
	job.Attempts++
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	if err := q.rdb.RPush(ctx, emailQueueKey, data).Err(); err != nil {
		return fmt.Errorf("requeue job: %w", err)
	}
	return nil
}

func (q *Queue) Len(ctx context.Context) (int64, error) {
	n, err := q.rdb.LLen(ctx, emailQueueKey).Result()
	if err != nil {
		return 0, fmt.Errorf("queue len: %w", err)
	}
	return n, nil
}
