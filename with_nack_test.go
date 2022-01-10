package messenger_amqp

import (
	"github.com/riid/messenger/envelope"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHasNack_given_envelope_without_nack_should_return_false(t *testing.T) {
	e := envelope.FromMessage("test message")
	res := HasNack(e)
	assert.False(t, res)
}

func TestHasNack_given_envelope_with_nack_should_return_true(t *testing.T) {
	e := WithNack(envelope.FromMessage("test message"))
	res := HasNack(e)
	assert.True(t, res)
}
