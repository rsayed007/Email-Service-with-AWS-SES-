package tracking

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// hrefRe matches absolute http/https href attribute values in HTML.
var hrefRe = regexp.MustCompile(`(?i)href="(https?://[^"]+)"`)

// Injector builds open-pixel and click-tracking URLs and injects them into
// HTML bodies. It uses simple logID path routing rather than signed tokens,
// so no shared secret is required between the injector and the handlers.
type Injector struct {
	baseURL string // e.g. "https://track.example.com" — no trailing slash
}

// NewInjector creates an Injector that generates tracking URLs rooted at baseURL.
func NewInjector(baseURL string) *Injector {
	return &Injector{baseURL: strings.TrimRight(baseURL, "/")}
}

// InjectOpenPixel appends a 1×1 transparent tracking pixel before </body>.
// If </body> is absent the pixel is appended at the end of htmlBody.
//
// The pixel URL format is: {baseURL}/o/{logID}
func (inj *Injector) InjectOpenPixel(htmlBody, logID string) string {
	pixelURL := fmt.Sprintf("%s/o/%s", inj.baseURL, logID)
	pixel := fmt.Sprintf(
		`<img src="%s" width="1" height="1" style="display:none" alt="">`,
		pixelURL,
	)
	if i := strings.LastIndex(strings.ToLower(htmlBody), "</body>"); i >= 0 {
		return htmlBody[:i] + pixel + htmlBody[i:]
	}
	return htmlBody + pixel
}

// InjectClickTracking replaces every absolute http/https href attribute with a
// click-tracking redirect URL that encodes the original destination.
//
// The redirect URL format is: {baseURL}/c/{logID}?u={url_encoded_original}
func (inj *Injector) InjectClickTracking(htmlBody, logID string) string {
	return hrefRe.ReplaceAllStringFunc(htmlBody, func(m string) string {
		sub := hrefRe.FindStringSubmatch(m)
		if len(sub) < 2 {
			return m
		}
		redirectURL := fmt.Sprintf(
			"%s/c/%s?u=%s",
			inj.baseURL,
			logID,
			url.QueryEscape(sub[1]),
		)
		return `href="` + redirectURL + `"`
	})
}
