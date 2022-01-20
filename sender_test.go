package messenger_amqp

import (
	"context"
	"errors"
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSender_Send_given_valid_amqp_channel_called_with_envelope_should_serialize_it_and_publish_to_channel(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2022, 01, 01, 00, 00, 00, 00, time.UTC)
	var e messenger.Envelope = envelope.FromMessage([]byte("test message"))
	e = envelope.WithContentType(e, "test-content-type")
	e = envelope.WithCorrelationID(e, "test-correlation-id")
	e = envelope.WithReplyTo(e, "test-reply-to")
	e = envelope.WithExpiration(e, 3*time.Minute)
	e = envelope.WithID(e, "test-message-id")
	e, _ = envelope.WithTimestamp(e, now)
	e = envelope.WithMessageType(e, "test-message-type")
	e = envelope.WithUserID(e, "test-user-id")
	e = envelope.WithAppID(e, "test-app-id")
	e = envelope.WithHeader(e, "x-custom-header", "test value")
	e = WithRoutingKey(e, "test-routing-key")

	expectedPublishing := amqp.Publishing{
		Headers: amqp.Table{
			"x-custom-header": []interface{}{"test value"},
		},
		ContentType:   "test-content-type",
		CorrelationId: "test-correlation-id",
		ReplyTo:       "test-reply-to",
		Expiration:    "180000",
		MessageId:     "test-message-id",
		Timestamp:     now,
		Type:          "test-message-type",
		UserId:        "test-user-id",
		AppId:         "test-app-id",
		Body:          []byte("test message"),
	}

	ch := &mockChannel{}
	ch.On("Publish", "test-exchange", "test-routing-key", false, false, expectedPublishing).Return(nil)

	sender := Sender(ch, PublishArgs{
		Exchange:  "test-exchange",
		Mandatory: false,
		Immediate: false,
	})

	err := sender.Send(ctx, e)

	assert.Nil(t, err)
	ch.AssertCalled(t, "Publish", "test-exchange", "test-routing-key", false, false, expectedPublishing)
}

func TestSender_Send_when_amqp_publish_fails_should_return_error(t *testing.T) {
	ctx := context.Background()
	var e messenger.Envelope = envelope.FromMessage([]byte("test message"))

	expectedPublishing := amqp.Publishing{
		Headers: amqp.Table{},
		Body:    []byte("test message"),
	}

	expectedError := errors.New("test error")

	ch := &mockChannel{}
	ch.On("Publish", "test-exchange", "", true, true, expectedPublishing).Return(expectedError)

	sender := Sender(ch, PublishArgs{
		Exchange:  "test-exchange",
		Mandatory: true,
		Immediate: true,
	})

	err := sender.Send(ctx, e)

	assert.Same(t, expectedError, err)
	ch.AssertCalled(t, "Publish", "test-exchange", "", true, true, expectedPublishing)
}
