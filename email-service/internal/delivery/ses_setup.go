package delivery

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
)

// EnsureConfigurationSet creates the per-client SES configuration set and
// SNS event destination if they do not already exist. Both operations are
// idempotent — AlreadyExists responses from SES are silently ignored.
func (d *SESDelivery) EnsureConfigurationSet(ctx context.Context, clientID string) error {
	name := configSetName(clientID)

	// Create the configuration set (idempotent).
	_, err := d.client.CreateConfigurationSet(ctx, &ses.CreateConfigurationSetInput{
		ConfigurationSet: &sestypes.ConfigurationSet{Name: aws.String(name)},
	})
	if err != nil {
		var alreadyExists *sestypes.ConfigurationSetAlreadyExistsException
		if !errors.As(err, &alreadyExists) {
			return fmt.Errorf("EnsureConfigurationSet create set: %w", err)
		}
	}

	// Register SNS event destination for all relevant event types.
	_, err = d.client.CreateConfigurationSetEventDestination(ctx,
		&ses.CreateConfigurationSetEventDestinationInput{
			ConfigurationSetName: aws.String(name),
			EventDestination: &sestypes.EventDestination{
				Name:    aws.String("sns-all-events"),
				Enabled: true,
				MatchingEventTypes: []sestypes.EventType{
					sestypes.EventTypeSend,
					sestypes.EventTypeDelivery,
					sestypes.EventTypeOpen,
					sestypes.EventTypeClick,
					sestypes.EventTypeBounce,
					sestypes.EventTypeComplaint,
				},
				SNSDestination: &sestypes.SNSDestination{
					TopicARN: aws.String(d.snsTopicARN),
				},
			},
		},
	)
	if err != nil {
		var alreadyExists *sestypes.EventDestinationAlreadyExistsException
		if !errors.As(err, &alreadyExists) {
			return fmt.Errorf("EnsureConfigurationSet event destination: %w", err)
		}
	}

	return nil
}
