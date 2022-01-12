package messenger_amqp

import (
	"context"
	"errors"
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/streadway/amqp"
	"strconv"
	"time"
)

type PublishArgs struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

func Sender(channel Channel, publishArgs PublishArgs) *sender {
	return &sender{
		channel:     channel,
		publishArgs: publishArgs,
	}
}

type sender struct {
	channel     Channel
	publishArgs PublishArgs
}

func (s *sender) Send(_ context.Context, e messenger.Envelope) error {
	msg, err := createAMQPMessageFromEnvelope(e)
	if err != nil {
		return err
	}

	err = s.channel.Publish(
		s.publishArgs.Exchange,
		s.publishArgs.RoutingKey,
		s.publishArgs.Mandatory,
		s.publishArgs.Immediate,
		msg,
	)

	if err != nil {
		return err
	}

	return nil
}

func createAMQPMessageFromEnvelope(e messenger.Envelope) (amqp.Publishing, error) {

	body, ok := e.Message().([]byte)
	if !ok {
		return amqp.Publishing{}, errors.New("message must be []byte")
	}

	contentType := envelope.ContentType(e)
	e = envelope.WithoutContentType(e)

	correlationID := envelope.CorrelationID(e)
	e = envelope.WithoutCorrelationID(e)

	replyTo := envelope.ReplyTo(e)
	e = envelope.WithoutReplyTo(e)

	expiration, err := envelope.Expiration(e)
	expirationStr := ""
	if err == nil {
		expirationStr = strconv.FormatInt(expiration.Milliseconds(), 10)
	}
	e = envelope.WithoutExpiration(e)

	id := envelope.ID(e)
	e = envelope.WithoutID(e)

	timestamp, err := envelope.Timestamp(e)
	if err == envelope.ErrNoTimestamp {
		timestamp = time.Time{}
	} else if err != nil {
		return amqp.Publishing{}, err
	}
	e = envelope.WithoutTimestamp(e)

	userID := envelope.UserID(e)
	e = envelope.WithoutUserID(e)

	appID := envelope.AppID(e)
	e = envelope.WithoutAppID(e)

	messageType := envelope.MessageType(e)
	e = envelope.WithoutMessageType(e)

	envelopeHeaders := e.Headers()
	headers := make(amqp.Table, len(envelopeHeaders))
	for name, hh := range e.Headers() {
		ii := make([]interface{}, len(hh))
		for i := 0; i < len(ii); i++ {
			ii[i] = hh[i]
		}

		headers[name] = ii
	}

	return amqp.Publishing{
		Headers:       headers,
		ContentType:   contentType,
		CorrelationId: correlationID,
		ReplyTo:       replyTo,
		Expiration:    expirationStr,
		MessageId:     id,
		Timestamp:     timestamp,
		Type:          messageType,
		UserId:        userID,
		AppId:         appID,
		Body:          body,
	}, nil
}
