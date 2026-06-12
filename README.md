# Email Service with AWS SES

Multi-tenant email delivery platform written in Go. Clients send mail via a REST API or an SMTP proxy; the background worker delivers through AWS SES and tracks every event (delivery, open, click, bounce, complaint) via AWS SNS webhooks.

---

## Architecture

```
Client (API key)
    │
    ▼
┌─────────┐    enqueue    ┌────────────┐    SES SendRawEmail    ┌─────────┐
│ REST API │ ──────────► │   Redis Q  │ ──────────────────────► │  AWS    │
│ :8080   │              └────────────┘                          │  SES    │
└─────────┘                     ▲                               └────┬────┘
                                │                                    │ SNS
┌─────────┐    enqueue          │                               ┌────▼────┐
│  SMTP   │ ───────────────────►│                               │  SNS    │
│  :2525  │              ┌──────┴─────┐                         │  Topic  │
└─────────┘              │   Worker   │                         └────┬────┘
                         └──────┬─────┘                              │
                                │                               ┌────▼────┐
                         ┌──────▼─────┐                         │Webhook  │
                         │   MySQL    │ ◄───────────────────────│/webhooks│
                         │   Redis    │                         │  /ses   │
                         └────────────┘                         └─────────┘
```

**Three runnable processes:**

| Binary | Port | Purpose |
|--------|------|---------|
| `cmd/api` | 8080 | REST API — send, list, stats, blacklist |
| `cmd/smtp` | 2525 | SMTP proxy — drop-in replacement for any SMTP client |
| `cmd/worker` | — | Dequeues jobs and calls AWS SES |

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.22+ |
| Docker + Docker Compose | any recent |
| AWS account | SES out of sandbox recommended |
| `migrate` CLI | optional — migrations also run on startup |

Install `golang-migrate` CLI (used by `make migrate-up`):

```bash
brew install golang-migrate          # macOS
# or
go install -tags 'mysql' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

---

## Project Layout

```
cmd/
  api/main.go          REST API server
  smtp/main.go         SMTP proxy server
  worker/main.go       Queue worker
internal/
  auth/                API key + SMTP credential validation (Redis-cached bcrypt)
  config/              Env-var config with startup validation
  delivery/            AWS SES SendRawEmail, MIME builder, config-set auto-create
  middleware/          Gin auth middleware
  queue/               Redis LPUSH/BRPOP queue with dead-letter support
  ratelimit/           Atomic dual-limit (hourly sliding window + monthly) via Lua
  repository/          MySQL CRUD for clients, email_logs, stats, blacklist
  tracking/            Open-pixel injection, click-link rewriting, tracking handlers
  webhook/             SNS RSA signature verification + SES event handler
migrations/            Plain SQL migrations (reference copy)
pkg/database/
  migrate.go           Runs embedded migrations at startup
  migrations/          golang-migrate up/down pairs (embedded in binary)
Dockerfile             Multi-stage: builder → api/smtp/worker scratch images
docker-compose.yml     MySQL 8, Redis 7, api, smtp, worker
Makefile               build / run / migrate / docker helpers
.env.example           All supported variables with comments
```

---

## Step 1 — Clone and configure

```bash
git clone <repo-url> email-service
cd email-service
cp .env.example .env
```

Open `.env` and fill in every value. The sections below explain what each block needs.

---

## Step 2 — AWS setup

### 2a — Verify a sending identity

In the AWS Console → **SES** → **Verified identities**, verify either:
- A domain (`example.com`) — recommended for production
- A single email address — sufficient for development

### 2b — Request production access (optional but recommended)

By default SES is in sandbox mode (can only send to verified addresses). To send to arbitrary recipients, open a support case to move out of sandbox.

### 2c — Create a configuration set

The worker creates per-client configuration sets automatically (`client-{id}`), but the service also needs a **default** configuration set for SNS event publishing.

1. AWS Console → SES → **Configuration sets** → Create configuration set
2. Name it exactly what you put in `SES_CONFIGURATION_SET` (e.g. `email-service-events`)
3. Add an **SNS destination** for all event types: Send, Delivery, Open, Click, Bounce, Complaint

### 2d — Create an SNS topic and subscribe the webhook

```bash
# Create topic
aws sns create-topic --name email-events --region us-east-1

# Subscribe — replace with your public URL
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:123456789012:email-events \
  --protocol https \
  --notification-endpoint https://your-domain.com/webhooks/ses
```

The API server auto-confirms the subscription when the `SubscriptionConfirmation` message arrives.

### 2e — IAM permissions

Create an IAM user or role with this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ses:SendRawEmail",
        "ses:CreateConfigurationSet",
        "ses:GetConfigurationSet"
      ],
      "Resource": "*"
    }
  ]
}
```

Put the access key and secret in `.env`.

---

## Step 3 — Fill in `.env`

Key variables to set (see `.env.example` for the full list with defaults):

```bash
# AWS
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
SES_CONFIGURATION_SET=email-service-events
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:email-events

# MySQL (used by Docker Compose and the app)
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=email_service
MYSQL_USER=email_user
MYSQL_PASSWORD=email_password
MYSQL_ROOT_PASSWORD=rootpassword

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=redispassword

# Tracking — public URL where the API is reachable
TRACKING_BASE_URL=https://your-domain.com
# Generate with: openssl rand -hex 32
TRACKING_HMAC_SECRET=replace_with_32_byte_hex_secret

# SMTP
SMTP_DOMAIN=mail.your-domain.com
```

---

## Step 4 — Start dependencies

```bash
docker compose up -d mysql redis
```

Wait for them to be healthy:

```bash
docker compose ps
```

---

## Step 5 — Run database migrations

Migrations run automatically when any service starts. To run them manually:

```bash
make migrate-up
# or directly:
migrate -path ./pkg/database/migrations \
  -database "mysql://email_user:email_password@tcp(localhost:3306)/email_service" up
```

This creates four tables: `clients`, `email_logs`, `email_daily_stats`, `blacklisted_emails`.

---

## Step 6 — Provision a client (tenant)

Each tenant needs a row in the `clients` table. The `api_key` and `smtp_password_hash` are generated manually and inserted directly.

### Generate credentials

```bash
# API key — a random string (prefix em_live_ is just a convention)
API_KEY="em_live_$(openssl rand -hex 20)"
echo $API_KEY

# SMTP password hash (bcrypt cost 12)
# Requires htpasswd or a small Go snippet:
go run -e 'package main
import (
  "fmt"
  "golang.org/x/crypto/bcrypt"
)
func main() {
  h, _ := bcrypt.GenerateFromPassword([]byte("your_smtp_password"), 12)
  fmt.Println(string(h))
}'
```

Or use a one-liner with Python:

```bash
python3 -c "import bcrypt; print(bcrypt.hashpw(b'your_smtp_password', bcrypt.gensalt(12)).decode())"
```

### Insert the client

```sql
INSERT INTO clients (
  id,
  name,
  smtp_username,
  smtp_password_hash,
  api_key,
  hourly_limit,
  monthly_limit,
  is_active
) VALUES (
  UUID(),
  'Acme Corp',
  'acme',
  '$2a$12$...',          -- bcrypt hash from above
  'em_live_...',         -- API key from above
  500,
  10000,
  1
);
```

---

## Step 7 — Start the services

### Option A — Local processes (development)

Open three terminals:

```bash
go run ./cmd/api
go run ./cmd/smtp
go run ./cmd/worker
```

Or with the Makefile:

```bash
make run-api    # terminal 1
make run-smtp   # terminal 2
make run-worker # terminal 3
```

### Option B — Docker Compose (full stack)

```bash
docker compose up -d --build
```

Services started:

| Container | Port |
|-----------|------|
| `email_service_mysql` | 3306 |
| `email_service_redis` | 6379 |
| `email_service_api` | 8080 |
| `email_service_smtp` | 2525 |
| `email_service_worker` | — |

---

## Step 8 — Optional: SMTP TLS (STARTTLS)

For STARTTLS support set both variables:

```bash
SMTP_TLS_CERT_FILE=/path/to/cert.pem
SMTP_TLS_KEY_FILE=/path/to/key.pem
```

Generate a self-signed cert for local testing:

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost"
```

When TLS is active, `SMTP_ALLOW_INSECURE_AUTH` is automatically disabled — AUTH is only allowed over encrypted connections.

For local dev without TLS:

```bash
SMTP_ALLOW_INSECURE_AUTH=true
```

---

## API Reference

All `/api/v1/*` routes require:

```
Authorization: Bearer <api_key>
```

### Health check

```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

### Send an email

```bash
curl -X POST http://localhost:8080/api/v1/emails \
  -H "Authorization: Bearer em_live_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "from": "sender@example.com",
    "to": ["recipient@example.com"],
    "subject": "Hello from the email service",
    "html_body": "<h1>Hello</h1><p>This is a test.</p>",
    "text_body": "Hello\n\nThis is a test."
  }'
```

Response (`202 Accepted`):

```json
{
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "remaining": 499
}
```

### List emails

```bash
curl http://localhost:8080/api/v1/emails \
  -H "Authorization: Bearer em_live_your_api_key"

# Filter by status
curl "http://localhost:8080/api/v1/emails?status=delivered" \
  -H "Authorization: Bearer em_live_your_api_key"
```

### Get one email

```bash
curl http://localhost:8080/api/v1/emails/<message_id> \
  -H "Authorization: Bearer em_live_your_api_key"
```

### Usage stats (last 30 days)

```bash
curl http://localhost:8080/api/v1/stats \
  -H "Authorization: Bearer em_live_your_api_key"
```

### Blacklist

```bash
# List
curl http://localhost:8080/api/v1/blacklist \
  -H "Authorization: Bearer em_live_your_api_key"

# Remove
curl -X DELETE http://localhost:8080/api/v1/blacklist/bad@example.com \
  -H "Authorization: Bearer em_live_your_api_key"
```

---

## SMTP Usage

The SMTP proxy listens on port `2525`. Use any standard SMTP client with:

- **Host**: localhost (or your server)
- **Port**: 2525
- **Username**: the `smtp_username` from the `clients` table
- **Password**: the plain-text password whose bcrypt hash is stored
- **Auth**: LOGIN

### Test with swaks

```bash
swaks \
  --to recipient@example.com \
  --from sender@example.com \
  --server localhost:2525 \
  --auth LOGIN \
  --auth-user acme \
  --auth-password your_smtp_password
```

### Test with curl

```bash
curl smtp://localhost:2525 \
  --user "acme:your_smtp_password" \
  --mail-from sender@example.com \
  --mail-rcpt recipient@example.com \
  --upload-file message.eml
```

---

## Email flow

```
1. Client sends POST /api/v1/emails (or SMTP)
2. Rate limits checked (hourly sliding window + monthly counter via Lua in Redis)
3. Blacklist checked for recipient
4. EmailLog created with status=queued
5. Job pushed to Redis queue (LPUSH queue:emails)
6. Worker pops job (BRPOP)
7. Worker checks blacklist again (belt-and-suspenders)
8. Open-pixel injected before </body>
9. Click links rewritten to redirect through /c/:logID
10. AWS SES SendRawEmail called
11. EmailLog updated with status=sent + aws_message_id
12. Daily stats incremented
13. SNS delivers event to POST /webhooks/ses
14. Webhook handler updates EmailLog status (delivered/bounced/etc)
15. Bounces and complaints auto-blacklist the address
```

---

## Database Schema

### clients

| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) | UUID, PK |
| name | VARCHAR(255) | Display name |
| smtp_username | VARCHAR(255) | Unique, used for SMTP AUTH |
| smtp_password_hash | VARCHAR(255) | bcrypt hash |
| api_key | VARCHAR(255) | Unique, Bearer token |
| hourly_limit | INT | Default 500 |
| monthly_limit | INT | Default 10000 |
| is_active | TINYINT(1) | 0 = suspended |
| created_at, updated_at | DATETIME | Auto-managed |

### email_logs

| Column | Type | Notes |
|--------|------|-------|
| id | CHAR(36) | UUID = tracking logID |
| client_id | CHAR(36) | FK → clients |
| aws_message_id | VARCHAR(255) | Set after SES accepts |
| from_email, to_email, subject | VARCHAR | |
| status | VARCHAR | queued/sent/delivered/opened/clicked/bounced/complained/failed |
| sent_at, delivered_at, opened_at, clicked_at, bounced_at | DATETIME | Nullable |

### email_daily_stats

Aggregated per client per UTC day. `PRIMARY KEY (client_id, date)` — rows are upserted on each event.

### blacklisted_emails

Hard bounces and complaints are auto-added. The REST API lets clients remove entries.

---

## Make targets

```bash
make build          # compile all three binaries to bin/
make run-api        # go run ./cmd/api
make run-smtp       # go run ./cmd/smtp
make run-worker     # go run ./cmd/worker
make test           # go test ./... -race
make fmt            # go fmt ./...
make vet            # go vet ./...
make tidy           # go mod tidy
make migrate-up     # apply pending DB migrations
make migrate-down   # roll back one migration
make migrate-version # show current version
make docker-up      # docker compose up -d --build
make docker-down    # docker compose down
make docker-logs    # docker compose logs -f
make clean          # rm -rf bin/
```

`migrate-*` targets read `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, and `MYSQL_DATABASE` from the environment (or from `.env` if you `export $(cat .env | xargs)` first).

---

## Troubleshooting

**`config: AWS_REGION is required`** — fill in all required env vars in `.env`; the app validates them on startup.

**`database connect failed`** — check `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`. Ensure MySQL is running and healthy (`docker compose ps`).

**`redis ping failed`** — verify `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` match the Compose stack.

**SMTP `535 Authentication credentials invalid`** — the stored `smtp_password_hash` is not a valid bcrypt hash of the password being sent, or `smtp_username` doesn't exist in `clients`.

**SMTP `530 Authentication required`** — TLS is enabled but the client isn't sending STARTTLS. Either configure TLS on the client or set `SMTP_ALLOW_INSECURE_AUTH=true` for local dev.

**Webhook events not arriving** — confirm the SNS subscription endpoint is your public API URL at `POST /webhooks/ses`, and that the subscription was confirmed (check API logs for "SNS subscription confirmed").

**Emails stuck in queue** — the worker is not running, or AWS credentials are wrong. Check worker logs for SES errors.

**Rate limit `429`** — the client has exceeded `hourly_limit` or `monthly_limit`. Increase the limits in the `clients` table or wait for the window to reset.

**`TRACKING_HMAC_SECRET` error** — generate a 32-byte hex secret: `openssl rand -hex 32`.
