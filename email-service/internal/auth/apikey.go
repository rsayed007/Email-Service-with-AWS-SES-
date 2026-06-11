package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"email-service/internal/repository"
)

const clientContextKey = "client"

type APIKeyAuthenticator struct {
	clients *repository.ClientRepository
}

func NewAPIKeyAuthenticator(clients *repository.ClientRepository) *APIKeyAuthenticator {
	return &APIKeyAuthenticator{clients: clients}
}

// Middleware extracts the Bearer token from Authorization header and loads the client.
func (a *APIKeyAuthenticator) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		apiKey, ok := parseBearerToken(header)
		if !ok {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing or invalid Authorization header"})
			return
		}

		client, err := a.clients.GetByAPIKey(c.Request.Context(), apiKey)
		if err != nil {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid api key"})
			return
		}

		c.Set(clientContextKey, client)
		c.Next()
	}
}

func ClientFromContext(c *gin.Context) (*repository.Client, error) {
	v, exists := c.Get(clientContextKey)
	if !exists {
		return nil, fmt.Errorf("client not in context")
	}
	client, ok := v.(*repository.Client)
	if !ok {
		return nil, fmt.Errorf("invalid client type in context")
	}
	return client, nil
}

func parseBearerToken(header string) (string, bool) {
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", false
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", false
	}
	return token, true
}

// GenerateAPIKey creates a cryptographically random API key.
func GenerateAPIKey() (string, error) {
	return generateRandomHex(32)
}

type contextKey string

const ClientKey contextKey = "client"

func ClientFromCtx(ctx context.Context) *repository.Client {
	v, _ := ctx.Value(ClientKey).(*repository.Client)
	return v
}
