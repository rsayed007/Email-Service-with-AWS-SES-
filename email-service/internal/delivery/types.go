package delivery

import "fmt"

// ── Job / result types ────────────────────────────────────────────────────────

// EmailJob is the delivery-layer representation of a single send request.
// Workers convert a queue.EmailJob into this type before calling SESDelivery.Send.
type EmailJob struct {
	// ClientID is the tenant identifier; used to name the per-client
	// SES configuration set ("client-{ClientID}").
	ClientID string
	// LogID is the email_logs.id UUID; forwarded to SES as a message tag.
	LogID string

	From    string
	To      []string
	ReplyTo string
	Subject string

	HTMLBody string // rendered HTML; takes priority over TextBody for rendering
	TextBody string // plain-text fallback

	// RawEmail, when non-empty, is sent as-is to SES and bypasses the MIME
	// builder. Useful when the caller has already assembled a full RFC 2822
	// message (e.g. forwarded from the SMTP proxy).
	RawEmail []byte
}

// SendResult is returned by SESDelivery.Send on success.
type SendResult struct {
	// MessageID is the SES-assigned message identifier (X-Message-Id).
	// It is used later to correlate SNS delivery/bounce/complaint events.
	MessageID string
	// Status is always "sent" on success; the SES event pipeline updates the
	// email_logs row to "delivered", "bounced", etc. via SNS webhooks.
	Status string
}

// ── Error types ───────────────────────────────────────────────────────────────

// SESErrorCode is a stable string that callers can switch on without importing
// AWS SDK types.
type SESErrorCode string

const (
	// ErrCodeMessageRejected — SES rejected the message (e.g. spam / virus policy).
	// Permanent: do not retry.
	ErrCodeMessageRejected SESErrorCode = "MESSAGE_REJECTED"

	// ErrCodeDomainNotVerified — the sending domain has not been verified in SES.
	// Permanent: operator must verify the domain before retrying.
	ErrCodeDomainNotVerified SESErrorCode = "DOMAIN_NOT_VERIFIED"

	// ErrCodeConfigSetNotFound — the per-client configuration set is missing.
	// The caller should call EnsureConfigurationSet and retry once.
	ErrCodeConfigSetNotFound SESErrorCode = "CONFIG_SET_NOT_FOUND"

	// ErrCodeAccountSuspended — sending has been disabled for the AWS account.
	// Permanent: operator must re-enable sending via the AWS console.
	ErrCodeAccountSuspended SESErrorCode = "ACCOUNT_SUSPENDED"

	// ErrCodeTransient — throttling or temporary service error. Retry with backoff.
	ErrCodeTransient SESErrorCode = "TRANSIENT"

	// ErrCodeUnknown — unclassified error; treat as retryable.
	ErrCodeUnknown SESErrorCode = "UNKNOWN"
)

// SESError is the interface satisfied by all custom delivery errors.
type SESError interface {
	error
	// Code returns the stable error code for switch statements.
	Code() SESErrorCode
	// IsRetryable reports whether the job should be re-queued.
	IsRetryable() bool
}

// ── Concrete error types ──────────────────────────────────────────────────────

// MessageRejectedError is returned when SES rejects the message.
type MessageRejectedError struct {
	Message string
}

func (e *MessageRejectedError) Error() string    { return "ses: message rejected: " + e.Message }
func (e *MessageRejectedError) Code() SESErrorCode { return ErrCodeMessageRejected }
func (e *MessageRejectedError) IsRetryable() bool  { return false }

// DomainNotVerifiedError is returned when the sender domain is not verified in SES.
type DomainNotVerifiedError struct {
	Domain  string
	Message string
}

func (e *DomainNotVerifiedError) Error() string {
	if e.Domain != "" {
		return fmt.Sprintf("ses: sending domain not verified: %s", e.Domain)
	}
	return "ses: sending domain not verified: " + e.Message
}
func (e *DomainNotVerifiedError) Code() SESErrorCode { return ErrCodeDomainNotVerified }
func (e *DomainNotVerifiedError) IsRetryable() bool  { return false }

// ConfigurationSetNotFoundError is returned when the per-client configuration
// set does not exist. Send() handles this automatically by calling
// EnsureConfigurationSet and retrying once.
type ConfigurationSetNotFoundError struct {
	ConfigurationSetName string
}

func (e *ConfigurationSetNotFoundError) Error() string {
	return fmt.Sprintf("ses: configuration set not found: %s", e.ConfigurationSetName)
}
func (e *ConfigurationSetNotFoundError) Code() SESErrorCode { return ErrCodeConfigSetNotFound }
func (e *ConfigurationSetNotFoundError) IsRetryable() bool  { return true }

// AccountSuspendedError is returned when email sending is disabled at the
// AWS-account level.
type AccountSuspendedError struct {
	Message string
}

func (e *AccountSuspendedError) Error() string    { return "ses: account sending disabled: " + e.Message }
func (e *AccountSuspendedError) Code() SESErrorCode { return ErrCodeAccountSuspended }
func (e *AccountSuspendedError) IsRetryable() bool  { return false }

// TransientError wraps a temporary SES / network failure that should be retried.
type TransientError struct {
	Cause error
}

func (e *TransientError) Error() string       { return "ses: transient error: " + e.Cause.Error() }
func (e *TransientError) Unwrap() error       { return e.Cause }
func (e *TransientError) Code() SESErrorCode  { return ErrCodeTransient }
func (e *TransientError) IsRetryable() bool   { return true }

// DeliveryError is the catch-all for unclassified SES errors.
type DeliveryError struct {
	Cause     error
	Retryable bool
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("ses: delivery error (retryable=%v): %v", e.Retryable, e.Cause)
}
func (e *DeliveryError) Unwrap() error       { return e.Cause }
func (e *DeliveryError) Code() SESErrorCode  { return ErrCodeUnknown }
func (e *DeliveryError) IsRetryable() bool   { return e.Retryable }
