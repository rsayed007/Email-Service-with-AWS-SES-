// Package middleware provides Gin middleware for the email-service HTTP API.
package middleware

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"email-service/internal/auth"
	"email-service/internal/repository"
)

// APIKeyMiddleware validates "Authorization: Bearer <key>" on every request.
//
// On success it stores the authenticated *repository.Client in the Gin context
// under the key accessible via auth.ClientFromContext. On failure it aborts
// with 401 and a JSON error body; no downstream handler is called.
func APIKeyMiddleware(a *auth.Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey, ok := parseBearerToken(c.GetHeader("Authorization"))
		if !ok {
			abortUnauthorized(c, "missing or malformed Authorization header (expected: Bearer <key>)")
			return
		}

		client, err := a.ValidateAPIKey(c.Request.Context(), apiKey)
		if err != nil {
			if isInvalidCredential(err) {
				abortUnauthorized(c, "invalid api key")
			} else {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": "authentication service unavailable",
				})
			}
			return
		}

		// Store client so downstream handlers can retrieve it.
		c.Set(auth.ClientContextKey(), client)
		c.Next()
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func parseBearerToken(header string) (string, bool) {
	const prefix = "bearer "
	if len(header) <= len(prefix) {
		return "", false
	}
	if !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}

func abortUnauthorized(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"error": gin.H{
			"code":    "UNAUTHORIZED",
			"message": message,
		},
	})
}

// isInvalidCredential returns true for errors that should map to 401 rather
// than 500. We distinguish them by message rather than type since the auth
// package uses sentinel errors only for repository misses.
func isInvalidCredential(err error) bool {
	return errors.Is(err, repository.ErrNotFound) ||
		strings.Contains(err.Error(), "invalid api key") ||
		strings.Contains(err.Error(), "api key is empty")
}
