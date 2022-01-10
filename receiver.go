package messenger_amqp

import (
	"context"
	"errors"
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/streadway/amqp"
	"strings"
)

var ErrInvalidAlias = errors.New("invalid alias")
var ErrNotAcked = errors.New("envelope is not acked")
var ErrNotNacked = errors.New("envelope is not nacked")

type ConsumeOptions struct {
	queue       string
	consumerTag string
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	args        amqp.Table
}

func Receiver(channel Channel, consumeOptions ConsumeOptions, alias string) *receiver {
	return &receiver{
		channel:        channel,
		consumeOptions: consumeOptions,
		alias:          alias,
	}
}

type receiver struct {
	channel        Channel
	consumeOptions ConsumeOptions
	alias          string
}

func (r *receiver) Receive(ctx context.Context) (<-chan messenger.Envelope, error) {
	ch, err := r.channel.Consume(
		r.consumeOptions.queue,
		r.consumeOptions.consumerTag,
		r.consumeOptions.autoAck,
		r.consumeOptions.exclusive,
		r.consumeOptions.noLocal,
		r.consumeOptions.noWait,
		r.consumeOptions.args,
	)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = r.channel.Cancel(r.consumeOptions.consumerTag, false)
	}()

	envelopes := make(chan messenger.Envelope)

	go func() {
		defer close(envelopes)
		for delivery := range ch {
			envelopes <- r.envelopeFromAMQPDelivery(delivery)
		}
	}()

	return envelopes, nil
}

func (r *receiver) Ack(_ context.Context, e messenger.Envelope) error {
	alias, deliveryTag, err := received(e)
	if err != nil {
		return err
	}
	if strings.Compare(alias, r.alias) != 0 {
		return ErrInvalidAlias
	}
	if !HasAck(e) {
		return ErrNotAcked
	}
	return r.channel.Ack(deliveryTag, false)
}

func (r *receiver) Nack(_ context.Context, e messenger.Envelope) error {
	alias, deliveryTag, err := received(e)
	if err != nil {
		return err
	}
	if strings.Compare(alias, r.alias) != 0 {
		return ErrInvalidAlias
	}
	if !HasNack(e) {
		return ErrNotNacked
	}
	return r.channel.Nack(deliveryTag, false, false)
}

func (r *receiver) Matches(e messenger.Envelope) bool {
	if r.consumeOptions.autoAck {
		return false
	}
	val, found := e.LastHeader(receiverAliasHeaderName)
	return found && strings.Compare(val, r.alias) == 0
}

func (r *receiver) envelopeFromAMQPDelivery(delivery amqp.Delivery) messenger.Envelope {
	var e messenger.Envelope = envelope.FromMessage(delivery.Body)

	e = withReceived(e, r.alias, delivery.DeliveryTag)

	if delivery.ContentType != "" {
		e = envelope.WithContentType(e, delivery.ContentType)
	}

	if delivery.CorrelationId != "" {
		e = envelope.WithCorrelationID(e, delivery.CorrelationId)
	}

	if delivery.ReplyTo != "" {
		e = envelope.WithReplyTo(e, delivery.ReplyTo)
	}

	if delivery.MessageId != "" {
		e = envelope.WithID(e, delivery.MessageId)
	}

	if !delivery.Timestamp.IsZero() {
		if withTS, err := envelope.WithTimestamp(e, delivery.Timestamp); err == nil {
			e = withTS
		}
	}

	if delivery.UserId != "" {
		e = envelope.WithUserID(e, delivery.UserId)
	}

	if delivery.AppId != "" {
		e = envelope.WithAppID(e, delivery.AppId)
	}

	if delivery.Type != "" {
		e = envelope.WithMessageType(e, delivery.Type)
	}

	headers := make(map[string][]string, len(delivery.Headers))
	for name, values := range delivery.Headers {
		vv, ok := values.([]interface{})
		if !ok {
			continue
		}

		headers[name] = make([]string, len(vv))
		for i, v := range vv {
			strValue, ok := v.(string)
			if !ok {
				continue
			}

			headers[name][i] = strValue
		}
	}

	return envelope.WithHeaders(e, headers)
}
