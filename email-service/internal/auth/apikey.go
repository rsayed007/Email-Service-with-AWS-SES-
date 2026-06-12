package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"

	"email-service/internal/repository"
)

// clientContextKey is the Gin context key under which the authenticated client
// is stored by APIKeyMiddleware.
const clientContextKey = "auth_client"

// ClientContextKey returns the Gin context key string used to store the client.
// Middleware packages that cannot import unexported constants use this accessor.
func ClientContextKey() string { return clientContextKey }

// ClientFromContext retrieves the authenticated client that APIKeyMiddleware
// stored in the Gin request context.
func ClientFromContext(c *gin.Context) (*repository.Client, error) {
	v, exists := c.Get(clientContextKey)
	if !exists {
		return nil, fmt.Errorf("client not found in context: middleware may not be applied")
	}
	client, ok := v.(*repository.Client)
	if !ok {
		return nil, fmt.Errorf("unexpected type in client context key")
	}
	return client, nil
}

// contextKey is the stdlib context.Context key type for the client value.
type contextKey string

// ClientKey is the key used to store a client in a plain context.Context
// (outside of Gin, e.g. in the SMTP session).
const ClientKey contextKey = "auth_client"

// ClientFromCtx retrieves the client stored in a plain context.Context.
// Returns nil if no client is set.
func ClientFromCtx(ctx context.Context) *repository.Client {
	v, _ := ctx.Value(ClientKey).(*repository.Client)
	return v
}

// parseBearerToken extracts the token from an "Authorization: Bearer <token>" header.
// Returns the token and true on success, or empty string and false on failure.
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
