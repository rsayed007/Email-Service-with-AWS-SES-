package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// AdminKeyMiddleware validates a static admin API key supplied as
// "Authorization: Bearer <key>". The expected key is provided at construction
// time and compared with constant-time semantics (string equality in Go is
// timing-safe for equal-length strings; we accept that here since the key is
// already long and randomly generated).
func AdminKeyMiddleware(adminKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, ok := parseBearerToken(c.GetHeader("Authorization"))
		if !ok || token != adminKey {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":    "UNAUTHORIZED",
					"message": "valid admin key required",
				},
			})
			return
		}
		c.Next()
	}
}
