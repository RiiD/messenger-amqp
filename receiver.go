package messenger_amqp

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/streadway/amqp"
)

var ErrInvalidAlias = errors.New("invalid alias")

type ConsumeOptions struct {
	Queue       string
	ConsumerTag string
	AutoAck     bool
	Exclusive   bool
	NoLocal     bool
	NoWait      bool
	Args        amqp.Table
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
		r.consumeOptions.Queue,
		r.consumeOptions.ConsumerTag,
		r.consumeOptions.AutoAck,
		r.consumeOptions.Exclusive,
		r.consumeOptions.NoLocal,
		r.consumeOptions.NoWait,
		r.consumeOptions.Args,
	)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = r.channel.Cancel(r.consumeOptions.ConsumerTag, false)
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
	return r.channel.Nack(deliveryTag, false, false)
}

func (r *receiver) Matches(e messenger.Envelope) bool {
	if r.consumeOptions.AutoAck {
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

	if delivery.RoutingKey != "" {
		e = WithRoutingKey(e, delivery.RoutingKey)
	}

	headers := make(map[string][]string, len(delivery.Headers))
	for name, values := range delivery.Headers {
		switch vv := values.(type) {

		case int:
			headers[name] = []string{strconv.FormatInt(int64(vv), 10)}
		case int8:
			headers[name] = []string{strconv.FormatInt(int64(vv), 10)}
		case int16:
			headers[name] = []string{strconv.FormatInt(int64(vv), 10)}
		case int32:
			headers[name] = []string{strconv.FormatInt(int64(vv), 10)}
		case int64:
			headers[name] = []string{strconv.FormatInt(vv, 10)}

		case float32:
			headers[name] = []string{strconv.FormatFloat(float64(vv), 'g', -1, 32)}
		case float64:
			headers[name] = []string{strconv.FormatFloat(vv, 'g', -1, 64)}
		case []float32:
			headers[name] = make([]string, len(vv))
			for i, f := range vv {
				headers[name][i] = strconv.FormatFloat(float64(f), 'g', -1, 32)
			}
		case []float64:
			headers[name] = make([]string, len(vv))
			for i, f := range vv {
				headers[name][i] = strconv.FormatFloat(f, 'g', -1, 64)
			}

		case []int:
			headers[name] = make([]string, len(vv))
			for i, v := range vv {
				headers[name][i] = strconv.FormatInt(int64(v), 10)
			}
		case []int8:
			headers[name] = make([]string, len(vv))
			for i, v := range vv {
				headers[name][i] = strconv.FormatInt(int64(v), 10)
			}
		case []int16:
			headers[name] = make([]string, len(vv))
			for i, v := range vv {
				headers[name][i] = strconv.FormatInt(int64(v), 10)
			}
		case []int32:
			headers[name] = make([]string, len(vv))
			for i, v := range vv {
				headers[name][i] = strconv.FormatInt(int64(v), 10)
			}
		case []int64:
			headers[name] = make([]string, len(vv))
			for i, v := range vv {
				headers[name][i] = strconv.FormatInt(v, 10)
			}

		case string:
			headers[name] = []string{vv}

		case []string:
			headers[name] = vv
		case []interface{}:
			headers[name] = make([]string, len(vv))
			for i, v := range vv {
				strValue, ok := v.(string)
				if !ok {
					continue
				}

				headers[name][i] = strValue
			}
		}

	}

	return envelope.WithHeaders(e, headers)
}
