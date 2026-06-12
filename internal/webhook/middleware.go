package webhook

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

// certCache avoids re-downloading the same AWS SNS signing certificate for
// every request. Certificates are immutable once published by AWS.
var certCache sync.Map

// SNSVerifyMiddleware returns a Gin handler that verifies the RSA signature on
// every inbound AWS SNS message.
//
// The body is read once, verified, then put back on c.Request.Body as a
// NopCloser so that downstream handlers can read it normally.
//
// Requests with an invalid signature are rejected with HTTP 403; malformed
// JSON is rejected with HTTP 400.
func SNSVerifyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, 1<<20))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "read body"})
			return
		}
		// Restore body so the handler can read it again.
		c.Request.Body = io.NopCloser(bytes.NewReader(body))

		var env snsEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
			return
		}

		if err := verifySNSSignature(&env); err != nil {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "invalid sns signature"})
			return
		}

		c.Next()
	}
}

// verifySNSSignature checks the RSA-PKCS1v15 signature on env.
// It supports both SignatureVersion "1" (SHA1) and "2" (SHA256).
func verifySNSSignature(env *snsEnvelope) error {
	if err := validateCertURL(env.SigningCertURL); err != nil {
		return err
	}

	cert, err := fetchCert(env.SigningCertURL)
	if err != nil {
		return err
	}

	pubKey, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("sns: signing cert has no RSA public key")
	}

	sig, err := base64.StdEncoding.DecodeString(env.Signature)
	if err != nil {
		return fmt.Errorf("sns: decode signature: %w", err)
	}

	msg := buildSignableMessage(env)

	switch env.SignatureVersion {
	case "1":
		digest := sha1.Sum(msg)
		return rsa.VerifyPKCS1v15(pubKey, crypto.SHA1, digest[:], sig)
	case "2":
		digest := sha256.Sum256(msg)
		return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, digest[:], sig)
	default:
		return fmt.Errorf("sns: unsupported SignatureVersion %q", env.SignatureVersion)
	}
}

// validateCertURL rejects any signing certificate URL that does not originate
// from an official AWS SNS host (sns.{region}.amazonaws.com).
func validateCertURL(rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("sns: empty SigningCertURL")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("sns: invalid SigningCertURL: %w", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("sns: SigningCertURL must use HTTPS")
	}
	host := u.Hostname()
	if !strings.HasPrefix(host, "sns.") || !strings.HasSuffix(host, ".amazonaws.com") {
		return fmt.Errorf("sns: untrusted SigningCertURL host %q", host)
	}
	return nil
}

// fetchCert downloads and caches the PEM certificate at certURL.
// Certificates are immutable at a given URL so caching them indefinitely is safe.
func fetchCert(certURL string) (*x509.Certificate, error) {
	if v, ok := certCache.Load(certURL); ok {
		return v.(*x509.Certificate), nil
	}

	resp, err := http.Get(certURL) //nolint:noctx // cert URL is always from AWS; no context needed
	if err != nil {
		return nil, fmt.Errorf("sns: download cert: %w", err)
	}
	defer resp.Body.Close()

	pemBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("sns: read cert: %w", err)
	}

	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("sns: no PEM block in cert response")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("sns: parse cert: %w", err)
	}

	certCache.Store(certURL, cert)
	return cert, nil
}

// buildSignableMessage constructs the canonical byte string used for SNS
// signature verification, following AWS SNS documentation.
//
// Notification fields (in this order):
//
//	Message, MessageId, Subject (if non-empty), Timestamp, TopicArn, Type
//
// SubscriptionConfirmation / UnsubscribeConfirmation fields:
//
//	Message, MessageId, SubscribeURL, Timestamp, Token, TopicArn, Type
func buildSignableMessage(env *snsEnvelope) []byte {
	var buf bytes.Buffer
	kv := func(k, v string) { fmt.Fprintf(&buf, "%s\n%s\n", k, v) }

	switch env.Type {
	case "Notification":
		kv("Message", env.Message)
		kv("MessageId", env.MessageID)
		if env.Subject != "" {
			kv("Subject", env.Subject)
		}
		kv("Timestamp", env.Timestamp)
		kv("TopicArn", env.TopicArn)
		kv("Type", env.Type)
	case "SubscriptionConfirmation", "UnsubscribeConfirmation":
		kv("Message", env.Message)
		kv("MessageId", env.MessageID)
		kv("SubscribeURL", env.SubscribeURL)
		kv("Timestamp", env.Timestamp)
		kv("Token", env.Token)
		kv("TopicArn", env.TopicArn)
		kv("Type", env.Type)
	}
	return buf.Bytes()
}
