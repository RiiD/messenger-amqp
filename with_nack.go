package messenger_amqp

import (
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
)

const nackHeaderName = "X-Messenger-AMQP-Nack"

func WithNack(e messenger.Envelope) messenger.Envelope {
	return envelope.WithHeader(e, nackHeaderName, "")
}

func HasNack(e messenger.Envelope) bool {
	return e.HasHeader(nackHeaderName)
}
