package tracking

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// Token is the signed payload embedded in open-pixel and click-redirect URLs.
type Token struct {
	LogID    string `json:"l"`
	ClientID string `json:"c"`
	URL      string `json:"u,omitempty"` // only for click tokens
}

type Tracker struct {
	secret  []byte
	baseURL string
	openPath  string
	clickPath string
}

func NewTracker(secret []byte, baseURL, openPath, clickPath string) *Tracker {
	return &Tracker{
		secret:    secret,
		baseURL:   strings.TrimRight(baseURL, "/"),
		openPath:  openPath,
		clickPath: clickPath,
	}
}

// OpenPixelURL returns a 1x1 pixel URL for open tracking.
func (t *Tracker) OpenPixelURL(logID, clientID string) (string, error) {
	tok := Token{LogID: logID, ClientID: clientID}
	signed, err := t.sign(tok)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s?t=%s", t.baseURL, t.openPath, signed), nil
}

// ClickURL wraps an original URL in a redirect for click tracking.
func (t *Tracker) ClickURL(logID, clientID, originalURL string) (string, error) {
	tok := Token{LogID: logID, ClientID: clientID, URL: originalURL}
	signed, err := t.sign(tok)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s?t=%s", t.baseURL, t.clickPath, signed), nil
}

// Verify decodes and verifies a tracking token, returning the payload.
func (t *Tracker) Verify(tokenStr string) (*Token, error) {
	parts := strings.SplitN(tokenStr, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid token format")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode signature: %w", err)
	}
	expected := t.hmac([]byte(parts[0]))
	if !hmac.Equal(sig, expected) {
		return nil, fmt.Errorf("invalid token signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	var tok Token
	if err := json.Unmarshal(payload, &tok); err != nil {
		return nil, fmt.Errorf("unmarshal token: %w", err)
	}
	return &tok, nil
}

func (t *Tracker) sign(tok Token) (string, error) {
	payload, err := json.Marshal(tok)
	if err != nil {
		return "", fmt.Errorf("marshal token: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	sig := t.hmac([]byte(encoded))
	sigEncoded := base64.RawURLEncoding.EncodeToString(sig)
	return encoded + "." + sigEncoded, nil
}

func (t *Tracker) hmac(data []byte) []byte {
	mac := hmac.New(sha256.New, t.secret)
	mac.Write(data)
	return mac.Sum(nil)
}
