package messenger_amqp

import (
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReceived_given_envelope_without_alias_or_delivery_tag_when_called_received_should_return_header_not_found_error(t *testing.T) {
	e := envelope.FromMessage("test message")
	alias, deliveryTag, err := received(e)

	assert.Empty(t, alias)
	assert.Empty(t, deliveryTag)
	assert.Same(t, envelope.ErrHeaderNotFound, err)
}

func TestReceived_given_envelope_with_alias_and_delivery_tag_when_called_received_should_return_alias_and_delivery_tag(t *testing.T) {
	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-alias", 123)
	alias, deliveryTag, err := received(e)

	assert.Equal(t, "test-alias", alias)
	assert.Equal(t, uint64(123), deliveryTag)
	assert.NoError(t, err)
}
