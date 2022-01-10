package messenger_amqp

import (
	"github.com/riid/messenger/envelope"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHasAck_given_envelope_without_ack_should_return_false(t *testing.T) {
	e := envelope.FromMessage("test message")
	res := HasAck(e)
	assert.False(t, res)
}

func TestHasAck_given_envelope_with_ack_should_return_true(t *testing.T) {
	e := WithAck(envelope.FromMessage("test message"))
	res := HasAck(e)
	assert.True(t, res)
}
