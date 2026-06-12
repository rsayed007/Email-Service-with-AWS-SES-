package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"email-service/internal/repository"
)

// ── SNS envelope ──────────────────────────────────────────────────────────────

// snsEnvelope is the outer JSON wrapper that AWS SNS sends for every message.
type snsEnvelope struct {
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Subject          string `json:"Subject,omitempty"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertURL   string `json:"SigningCertURL"`
	SubscribeURL     string `json:"SubscribeURL,omitempty"`
	Token            string `json:"Token,omitempty"`
	UnsubscribeURL   string `json:"UnsubscribeURL,omitempty"`
}

// ── SES notification payload ──────────────────────────────────────────────────

// sesNotification is the inner JSON carried in snsEnvelope.Message for
// SES configuration-set events. Configuration sets use "eventType"; older
// direct SNS notifications use "notificationType".
type sesNotification struct {
	EventType        string        `json:"eventType"`
	NotificationType string        `json:"notificationType"`
	Mail             sesMail       `json:"mail"`
	Bounce           *sesBounce    `json:"bounce,omitempty"`
	Complaint        *sesComplaint `json:"complaint,omitempty"`
	Delivery         *sesDelivery  `json:"delivery,omitempty"`
	Open             *sesOpen      `json:"open,omitempty"`
	Click            *sesClick     `json:"click,omitempty"`
}

// kind returns the effective event type, preferring the configuration-set
// field over the legacy direct-notification field.
func (n *sesNotification) kind() string {
	if n.EventType != "" {
		return n.EventType
	}
	return n.NotificationType
}

type sesMail struct {
	MessageID   string   `json:"messageId"`
	Destination []string `json:"destination"`
}

type sesBounce struct {
	BounceType        string            `json:"bounceType"`
	BounceSubType     string            `json:"bounceSubType"`
	BouncedRecipients []bounceRecipient `json:"bouncedRecipients"`
	Timestamp         time.Time         `json:"timestamp"`
}

type bounceRecipient struct {
	EmailAddress string `json:"emailAddress"`
}

type sesComplaint struct {
	ComplainedRecipients  []complaintRecipient `json:"complainedRecipients"`
	Timestamp             time.Time            `json:"timestamp"`
	ComplaintFeedbackType string               `json:"complaintFeedbackType,omitempty"`
}

type complaintRecipient struct {
	EmailAddress string `json:"emailAddress"`
}

type sesDelivery struct {
	Timestamp            time.Time `json:"timestamp"`
	ProcessingTimeMillis int64     `json:"processingTimeMillis"`
	Recipients           []string  `json:"recipients"`
	SMTPResponse         string    `json:"smtpResponse"`
}

type sesOpen struct {
	Timestamp time.Time `json:"timestamp"`
	IPAddress string    `json:"ipAddress"`
	UserAgent string    `json:"userAgent"`
}

type sesClick struct {
	Timestamp time.Time `json:"timestamp"`
	IPAddress string    `json:"ipAddress"`
	UserAgent string    `json:"userAgent"`
	Link      string    `json:"link"`
}

// ── Handler ───────────────────────────────────────────────────────────────────

// SNSHandler processes SES event notifications delivered via AWS SNS.
type SNSHandler struct {
	emailLogs *repository.EmailLogRepository
	stats     *repository.StatsRepository
	blacklist *repository.BlacklistRepository
	logger    *slog.Logger
}

// NewSNSHandler creates a SNSHandler backed by the given repositories.
func NewSNSHandler(
	emailLogs *repository.EmailLogRepository,
	stats *repository.StatsRepository,
	blacklist *repository.BlacklistRepository,
	logger *slog.Logger,
) *SNSHandler {
	return &SNSHandler{
		emailLogs: emailLogs,
		stats:     stats,
		blacklist: blacklist,
		logger:    logger,
	}
}

// Handle is the Gin handler for POST /webhooks/ses.
// It dispatches on the SNS message type.
func (h *SNSHandler) Handle(c *gin.Context) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 1<<20))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body"})
		return
	}

	var env snsEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sns envelope"})
		return
	}

	switch env.Type {
	case "SubscriptionConfirmation":
		if err := h.confirmSubscription(c.Request.Context(), env.SubscribeURL); err != nil {
			h.logger.Error("sns subscription confirmation failed",
				"topic_arn", env.TopicArn, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "confirm subscription"})
			return
		}
		h.logger.Info("sns subscription confirmed", "topic_arn", env.TopicArn)

	case "Notification":
		var notif sesNotification
		if err := json.Unmarshal([]byte(env.Message), &notif); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ses notification"})
			return
		}
		if err := h.processNotification(c.Request.Context(), &notif); err != nil {
			h.logger.Error("ses notification processing failed",
				"event_type", notif.kind(), "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

	case "UnsubscribeConfirmation":
		h.logger.Warn("sns unsubscribe confirmation received", "topic_arn", env.TopicArn)

	default:
		h.logger.Debug("ignored sns message type", "type", env.Type)
	}

	c.Status(http.StatusOK)
}

// processNotification handles a single SES event notification.
func (h *SNSHandler) processNotification(ctx context.Context, notif *sesNotification) error {
	log, err := h.emailLogs.GetByAWSMessageID(ctx, notif.Mail.MessageID)
	if err != nil {
		// Unknown message ID is not a service error — SES may emit events for
		// messages sent before the service started or via other channels.
		return nil
	}

	now := time.Now().UTC()

	switch notif.kind() {
	case "Send":
		// Safety net: the worker already set this, but confirm here in case of a gap.
		return h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusSent)

	case "Delivery":
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusDelivered); err != nil {
			return fmt.Errorf("update delivered: %w", err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "delivered")

	case "Open":
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusOpened); err != nil {
			return fmt.Errorf("update opened: %w", err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "opened")

	case "Click":
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusClicked); err != nil {
			return fmt.Errorf("update clicked: %w", err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "clicked")

	case "Bounce":
		if notif.Bounce == nil {
			return nil
		}
		status := repository.StatusSoftBounced
		if notif.Bounce.BounceType == "Permanent" {
			status = repository.StatusHardBounced
			for _, r := range notif.Bounce.BouncedRecipients {
				_ = h.blacklist.Add(ctx, &repository.BlacklistedEmail{
					ClientID: log.ClientID,
					Email:    r.EmailAddress,
					Reason: fmt.Sprintf("hard_bounce:%s/%s",
						notif.Bounce.BounceType, notif.Bounce.BounceSubType),
				})
			}
		}
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, status); err != nil {
			return fmt.Errorf("update %s: %w", status, err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "bounced")

	case "Complaint":
		if notif.Complaint == nil {
			return nil
		}
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusComplained); err != nil {
			return fmt.Errorf("update complained: %w", err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "complained")
		for _, r := range notif.Complaint.ComplainedRecipients {
			_ = h.blacklist.Add(ctx, &repository.BlacklistedEmail{
				ClientID: log.ClientID,
				Email:    r.EmailAddress,
				Reason:   "complaint:" + notif.Complaint.ComplaintFeedbackType,
			})
		}
		// Complaints are a leading indicator of reputation damage — log at ERROR
		// so that on-call alerting fires before SES suspends the account.
		h.logger.Error("complaint received — sender reputation risk",
			"log_id", log.ID,
			"client_id", log.ClientID,
			"to_email", log.ToEmail,
			"feedback_type", notif.Complaint.ComplaintFeedbackType,
		)
	}

	return nil
}

func (h *SNSHandler) confirmSubscription(ctx context.Context, subscribeURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, subscribeURL, nil)
	if err != nil {
		return fmt.Errorf("build confirm request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("confirm subscription: %w", err)
	}
	resp.Body.Close()
	return nil
}
