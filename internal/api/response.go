// Package api provides standard HTTP response helpers for the Gin router.
// Every success response is wrapped in { "data": ..., "meta": ... }.
// Every error response is wrapped in { "error": { "code": ..., "message": ... } }.
package api

import (
	"math"
	"net/http"

	"github.com/gin-gonic/gin"
)

// PaginationMeta is the meta block appended to paginated list responses.
type PaginationMeta struct {
	Page  int   `json:"page"`
	Limit int   `json:"limit"`
	Total int64 `json:"total"`
	Pages int   `json:"pages"`
}

type errBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// OK sends HTTP 200 with { "data": v }.
func OK(c *gin.Context, v any) {
	c.JSON(http.StatusOK, gin.H{"data": v})
}

// Created sends HTTP 201 with { "data": v }.
func Created(c *gin.Context, v any) {
	c.JSON(http.StatusCreated, gin.H{"data": v})
}

// Accepted sends HTTP 202 with { "data": v }.
func Accepted(c *gin.Context, v any) {
	c.JSON(http.StatusAccepted, gin.H{"data": v})
}

// Page sends HTTP 200 with { "data": v, "meta": { page, limit, total, pages } }.
func Page(c *gin.Context, v any, page, limit int, total int64) {
	pages := int(math.Ceil(float64(total) / float64(limit)))
	if pages < 1 {
		pages = 1
	}
	c.JSON(http.StatusOK, gin.H{
		"data": v,
		"meta": PaginationMeta{Page: page, Limit: limit, Total: total, Pages: pages},
	})
}

// Err aborts the request and sends { "error": { "code": code, "message": message } }.
func Err(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, gin.H{
		"error": errBody{Code: code, Message: message},
	})
}
