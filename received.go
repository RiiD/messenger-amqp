package messenger_amqp

import (
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
)

const deliveryTagHeader = "X-Messenger-AMQP-Delivery-Tag"
const receiverAliasHeaderName = "X-Messenger-AMQP-Receiver-Alias"

func withReceived(wrapped messenger.Envelope, alias string, deliveryTag uint64) messenger.Envelope {
	e := envelope.WithUint64(wrapped, deliveryTagHeader, deliveryTag)
	e = envelope.WithHeader(e, receiverAliasHeaderName, alias)
	return e
}

func received(e messenger.Envelope) (string, uint64, error) {
	deliveryTag, err := envelope.Uint64(e, deliveryTagHeader)
	if err != nil {
		return "", 0, err
	}

	alias, found := e.LastHeader(receiverAliasHeaderName)
	if !found {
		return "", 0, envelope.ErrHeaderNotFound
	}

	return alias, deliveryTag, nil
}
