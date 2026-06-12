package tracking

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"

	"email-service/internal/repository"
)

// transparentGIF is the raw bytes of a 1×1 transparent GIF image.
var transparentGIF = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00,
	0x80, 0x00, 0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21,
	0xf9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00,
	0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3b,
}

// Handlers provides Gin HTTP handlers for the open-pixel and click-redirect
// tracking endpoints. Database writes run in background goroutines so the
// HTTP response is never delayed by persistence latency.
type Handlers struct {
	logs   *repository.EmailLogRepository
	stats  *repository.StatsRepository
	logger *slog.Logger
}

// NewHandlers creates a Handlers backed by the given repositories.
func NewHandlers(
	logs *repository.EmailLogRepository,
	stats *repository.StatsRepository,
	logger *slog.Logger,
) *Handlers {
	return &Handlers{logs: logs, stats: stats, logger: logger}
}

// HandleOpen serves the 1×1 open-tracking pixel and records the open event.
//
// Route: GET /o/:logID
//
// The GIF is written immediately; the database update and stats increment run
// in a background goroutine using context.Background() so the response is
// never held while waiting for persistence.
func (h *Handlers) HandleOpen(c *gin.Context) {
	logID := c.Param("logID")
	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Data(http.StatusOK, "image/gif", transparentGIF)

	go h.recordOpen(logID)
}

// HandleClick redirects the visitor to the original destination URL and
// records the click event asynchronously.
//
// Route: GET /c/:logID?u={url_encoded_destination}
func (h *Handlers) HandleClick(c *gin.Context) {
	logID := c.Param("logID")
	raw := c.Query("u")

	destURL, err := url.QueryUnescape(raw)
	if err != nil || destURL == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	c.Redirect(http.StatusFound, destURL)

	go h.recordClick(logID)
}

func (h *Handlers) recordOpen(logID string) {
	ctx := context.Background()
	if err := h.logs.UpdateStatus(ctx, logID, repository.StatusOpened); err != nil {
		h.logger.Warn("open tracking: update status failed", "log_id", logID, "error", err)
		return
	}
	log, err := h.logs.GetByID(ctx, logID)
	if err != nil {
		h.logger.Warn("open tracking: get log failed", "log_id", logID, "error", err)
		return
	}
	_ = h.stats.IncrementStat(ctx, log.ClientID, time.Now().UTC(), "opened")
}

func (h *Handlers) recordClick(logID string) {
	ctx := context.Background()
	if err := h.logs.UpdateStatus(ctx, logID, repository.StatusClicked); err != nil {
		h.logger.Warn("click tracking: update status failed", "log_id", logID, "error", err)
		return
	}
	log, err := h.logs.GetByID(ctx, logID)
	if err != nil {
		h.logger.Warn("click tracking: get log failed", "log_id", logID, "error", err)
		return
	}
	_ = h.stats.IncrementStat(ctx, log.ClientID, time.Now().UTC(), "clicked")
}
