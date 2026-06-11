package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"email-service/internal/repository"
)

// SNSHandler processes bounce/complaint/delivery notifications from AWS SNS.
type SNSHandler struct {
	emailLogs *repository.EmailLogRepository
	stats     *repository.StatsRepository
	blacklist *repository.BlacklistRepository
}

func NewSNSHandler(
	emailLogs *repository.EmailLogRepository,
	stats *repository.StatsRepository,
	blacklist *repository.BlacklistRepository,
) *SNSHandler {
	return &SNSHandler{
		emailLogs: emailLogs,
		stats:     stats,
		blacklist: blacklist,
	}
}

type snsEnvelope struct {
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Message          string `json:"Message"`
	SubscribeURL     string `json:"SubscribeURL"`
	Token            string `json:"Token"`
}

type sesNotification struct {
	NotificationType string          `json:"notificationType"`
	Mail             sesMail         `json:"mail"`
	Bounce           *sesBounce      `json:"bounce,omitempty"`
	Complaint        *sesComplaint   `json:"complaint,omitempty"`
	Delivery         *sesDelivery    `json:"delivery,omitempty"`
}

type sesMail struct {
	MessageID string   `json:"messageId"`
	Destination []string `json:"destination"`
}

type sesBounce struct {
	BounceType        string             `json:"bounceType"`
	BounceSubType     string             `json:"bounceSubType"`
	BouncedRecipients []bounceRecipient  `json:"bouncedRecipients"`
	Timestamp         time.Time          `json:"timestamp"`
}

type bounceRecipient struct {
	EmailAddress string `json:"emailAddress"`
}

type sesComplaint struct {
	ComplainedRecipients []complaintRecipient `json:"complainedRecipients"`
	Timestamp            time.Time            `json:"timestamp"`
}

type complaintRecipient struct {
	EmailAddress string `json:"emailAddress"`
}

type sesDelivery struct {
	Timestamp time.Time `json:"timestamp"`
	Recipients []string  `json:"recipients"`
}

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

	// SNS subscription confirmation — auto-confirm by visiting SubscribeURL.
	if env.Type == "SubscriptionConfirmation" {
		if err := h.confirmSubscription(c.Request.Context(), env.SubscribeURL); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "confirm subscription"})
			return
		}
		c.Status(http.StatusOK)
		return
	}

	if env.Type != "Notification" {
		c.Status(http.StatusOK)
		return
	}

	var notif sesNotification
	if err := json.Unmarshal([]byte(env.Message), &notif); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ses notification"})
		return
	}

	if err := h.processNotification(c.Request.Context(), &notif); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}

func (h *SNSHandler) processNotification(ctx context.Context, notif *sesNotification) error {
	log, err := h.emailLogs.GetByAWSMessageID(ctx, notif.Mail.MessageID)
	if err != nil {
		// Unknown message — not an error we need to surface.
		return nil
	}

	now := time.Now().UTC()

	switch notif.NotificationType {
	case "Delivery":
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusDelivered); err != nil {
			return fmt.Errorf("update delivered: %w", err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "delivered")

	case "Bounce":
		if notif.Bounce == nil {
			return nil
		}
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusBounced); err != nil {
			return fmt.Errorf("update bounced: %w", err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "bounced")

		// Permanently bounce → add to blacklist.
		if notif.Bounce.BounceType == "Permanent" {
			for _, r := range notif.Bounce.BouncedRecipients {
				entry := &repository.BlacklistedEmail{
					ClientID: log.ClientID,
					Email:    r.EmailAddress,
					Reason:   fmt.Sprintf("bounce:%s/%s", notif.Bounce.BounceType, notif.Bounce.BounceSubType),
				}
				_ = h.blacklist.Add(ctx, entry)
			}
		}

	case "Complaint":
		if notif.Complaint == nil {
			return nil
		}
		if err := h.emailLogs.UpdateStatus(ctx, log.ID, repository.StatusComplained); err != nil {
			return fmt.Errorf("update complained: %w", err)
		}
		_ = h.stats.IncrementStat(ctx, log.ClientID, now, "complained")

		for _, r := range notif.Complaint.ComplainedRecipients {
			entry := &repository.BlacklistedEmail{
				ClientID: log.ClientID,
				Email:    r.EmailAddress,
				Reason:   "complaint",
			}
			_ = h.blacklist.Add(ctx, entry)
		}
	}

	return nil
}

func (h *SNSHandler) confirmSubscription(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
