package messenger_amqp

import (
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
)

const ackHeaderName = "X-Messenger-AMQP-Ack"

func WithAck(e messenger.Envelope) messenger.Envelope {
	return envelope.WithHeader(e, ackHeaderName, "")
}

func HasAck(e messenger.Envelope) bool {
	return e.HasHeader(ackHeaderName)
}
