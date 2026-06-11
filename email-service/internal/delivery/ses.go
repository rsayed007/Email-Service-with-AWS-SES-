package delivery

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
)

type SESClient struct {
	client           *ses.Client
	configurationSet string
}

func NewSESClient(awsCfg aws.Config, configurationSet string) *SESClient {
	return &SESClient{
		client:           ses.NewFromConfig(awsCfg),
		configurationSet: configurationSet,
	}
}

type SendRequest struct {
	From             string
	To               []string
	ReplyTo          []string
	Subject          string
	HTMLBody         string
	TextBody         string
	// Tags are forwarded to SES for per-message classification
	Tags             map[string]string
}

type SendResult struct {
	MessageID string
}

func (s *SESClient) Send(ctx context.Context, req SendRequest) (*SendResult, error) {
	input := &ses.SendEmailInput{
		Source: aws.String(req.From),
		Destination: &types.Destination{
			ToAddresses: req.To,
		},
		Message: &types.Message{
			Subject: &types.Content{
				Data:    aws.String(req.Subject),
				Charset: aws.String("UTF-8"),
			},
			Body: buildBody(req.HTMLBody, req.TextBody),
		},
	}

	if len(req.ReplyTo) > 0 {
		input.ReplyToAddresses = req.ReplyTo
	}

	if s.configurationSet != "" {
		input.ConfigurationSetName = aws.String(s.configurationSet)
	}

	if len(req.Tags) > 0 {
		for k, v := range req.Tags {
			input.Tags = append(input.Tags, types.MessageTag{
				Name:  aws.String(sanitizeTagValue(k)),
				Value: aws.String(sanitizeTagValue(v)),
			})
		}
	}

	out, err := s.client.SendEmail(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("ses send email: %w", err)
	}

	return &SendResult{MessageID: aws.ToString(out.MessageId)}, nil
}

func buildBody(html, text string) *types.Body {
	body := &types.Body{}
	if html != "" {
		body.Html = &types.Content{Data: aws.String(html), Charset: aws.String("UTF-8")}
	}
	if text != "" {
		body.Text = &types.Content{Data: aws.String(text), Charset: aws.String("UTF-8")}
	}
	return body
}

// SES tag values may only contain ASCII letters, digits, underscores, or hyphens.
func sanitizeTagValue(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}
