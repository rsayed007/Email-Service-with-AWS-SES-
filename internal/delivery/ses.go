package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"mime/quotedprintable"
	"net/textproto"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/smithy-go"
)

// SESDelivery sends emails through AWS SES v1 (2010-12-01) using raw send.
type SESDelivery struct {
	client      *ses.Client
	snsTopicARN string
}

// NewSESDelivery creates a SESDelivery. snsTopicARN is stored so that
// EnsureConfigurationSet can register SNS event destinations for new clients.
func NewSESDelivery(awsCfg aws.Config, snsTopicARN string) *SESDelivery {
	return &SESDelivery{
		client:      ses.NewFromConfig(awsCfg),
		snsTopicARN: snsTopicARN,
	}
}

// configSetName returns the SES configuration set name for a given client.
func configSetName(clientID string) string {
	return "client-" + clientID
}

// Send delivers job through SES. If the per-client configuration set is
// absent, Send auto-creates it via EnsureConfigurationSet and retries once.
func (d *SESDelivery) Send(ctx context.Context, job *EmailJob) (*SendResult, error) {
	result, err := d.send(ctx, job)
	if err == nil {
		return result, nil
	}

	var csErr *ConfigurationSetNotFoundError
	if errors.As(err, &csErr) {
		if setupErr := d.EnsureConfigurationSet(ctx, job.ClientID); setupErr != nil {
			return nil, setupErr
		}
		return d.send(ctx, job)
	}

	return nil, err
}

// send executes a single SendRawEmail call with no retry logic.
func (d *SESDelivery) send(ctx context.Context, job *EmailJob) (*SendResult, error) {
	raw, err := buildRawMessage(job)
	if err != nil {
		return nil, &DeliveryError{
			Cause:     fmt.Errorf("build MIME message: %w", err),
			Retryable: false,
		}
	}

	input := &ses.SendRawEmailInput{
		Source:               aws.String(job.From),
		Destinations:         job.To,
		RawMessage:           &sestypes.RawMessage{Data: raw},
		ConfigurationSetName: aws.String(configSetName(job.ClientID)),
		Tags: []sestypes.MessageTag{
			{Name: aws.String("client_id"), Value: aws.String(sanitizeTag(job.ClientID))},
			{Name: aws.String("log_id"), Value: aws.String(sanitizeTag(job.LogID))},
		},
	}

	out, err := d.client.SendRawEmail(ctx, input)
	if err != nil {
		return nil, classifyError(err, job.ClientID)
	}

	return &SendResult{
		MessageID: aws.ToString(out.MessageId),
		Status:    "sent",
	}, nil
}

// buildRawMessage constructs a MIME / RFC 2822 message from an EmailJob.
// If job.RawEmail is non-empty it is returned unchanged.
func buildRawMessage(job *EmailJob) ([]byte, error) {
	if len(job.RawEmail) > 0 {
		return job.RawEmail, nil
	}

	var buf bytes.Buffer

	// Common headers.
	fmt.Fprintf(&buf, "From: %s\r\n", job.From)
	fmt.Fprintf(&buf, "To: %s\r\n", strings.Join(job.To, ", "))
	fmt.Fprintf(&buf, "Subject: %s\r\n", job.Subject)
	if job.ReplyTo != "" {
		fmt.Fprintf(&buf, "Reply-To: %s\r\n", job.ReplyTo)
	}
	fmt.Fprintf(&buf, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z))
	fmt.Fprintf(&buf, "MIME-Version: 1.0\r\n")

	switch {
	case job.HTMLBody != "" && job.TextBody != "":
		// Build the multipart body first so we know the boundary before we
		// write the Content-Type header.
		var bodyBuf bytes.Buffer
		mw := multipart.NewWriter(&bodyBuf)

		if err := writeQPPart(mw, "text/plain", job.TextBody); err != nil {
			return nil, err
		}
		if err := writeQPPart(mw, "text/html", job.HTMLBody); err != nil {
			return nil, err
		}
		if err := mw.Close(); err != nil {
			return nil, err
		}

		// boundary is stable after NewWriter; mw.Close() doesn't change it.
		fmt.Fprintf(&buf, "Content-Type: multipart/alternative; boundary=%q\r\n\r\n", mw.Boundary())
		buf.Write(bodyBuf.Bytes())

	case job.HTMLBody != "":
		buf.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
		buf.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")
		if err := writeQP(&buf, job.HTMLBody); err != nil {
			return nil, err
		}

	default:
		buf.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		buf.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")
		if err := writeQP(&buf, job.TextBody); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// writeQPPart appends a quoted-printable MIME part to mw.
func writeQPPart(mw *multipart.Writer, mediaType, body string) error {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", mediaType+"; charset=UTF-8")
	h.Set("Content-Transfer-Encoding", "quoted-printable")
	pw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	return writeQP(pw, body)
}

// writeQP encodes body as quoted-printable into w.
func writeQP(w io.Writer, body string) error {
	qw := quotedprintable.NewWriter(w)
	if _, err := qw.Write([]byte(body)); err != nil {
		return err
	}
	return qw.Close()
}

// classifyError maps an AWS SES error to a structured SESError type.
func classifyError(err error, clientID string) SESError {
	var msgRej *sestypes.MessageRejected
	if errors.As(err, &msgRej) {
		return &MessageRejectedError{Message: msgRej.Error()}
	}

	var domainErr *sestypes.MailFromDomainNotVerifiedException
	if errors.As(err, &domainErr) {
		return &DomainNotVerifiedError{Message: domainErr.Error()}
	}

	var csNotExist *sestypes.ConfigurationSetDoesNotExistException
	if errors.As(err, &csNotExist) {
		return &ConfigurationSetNotFoundError{ConfigurationSetName: configSetName(clientID)}
	}

	var acctPaused *sestypes.AccountSendingPausedException
	if errors.As(err, &acctPaused) {
		return &AccountSuspendedError{Message: acctPaused.Error()}
	}

	// Throttling and temporary service errors — retry with backoff.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "Throttling" || code == "RequestLimitExceeded" ||
			strings.Contains(strings.ToLower(code), "throttl") {
			return &TransientError{Cause: err}
		}
	}

	return &DeliveryError{Cause: err, Retryable: true}
}

// sanitizeTag replaces characters disallowed in SES message tag values with underscores.
func sanitizeTag(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}
