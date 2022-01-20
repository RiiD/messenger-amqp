package messenger_amqp

import (
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
)

const routingKeyHeaderName = "X-Messenger-AMQP-Routing-Key"

func WithRoutingKey(e messenger.Envelope, key string) messenger.Envelope {
	return envelope.WithHeader(e, routingKeyHeaderName, key)
}

func WithoutRoutingKey(e messenger.Envelope) messenger.Envelope {
	return envelope.WithoutHeader(e, routingKeyHeaderName)
}

func RoutingKey(e messenger.Envelope) string {
	routingKey, _ := e.LastHeader(routingKeyHeaderName)
	return routingKey
}
