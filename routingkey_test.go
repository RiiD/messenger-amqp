package messenger_amqp

import (
	"github.com/riid/messenger/envelope"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWithRoutingKey_will_add_routing_key_header_to_envelope(t *testing.T) {
	e := envelope.FromMessage("test message")
	eWithRoutingKey := WithRoutingKey(e, "test key")

	routingKey, found := eWithRoutingKey.LastHeader(routingKeyHeaderName)
	assert.True(t, found)
	assert.Equal(t, "test key", routingKey)

	assert.True(t, eWithRoutingKey.Is(e))
}

func TestRoutingKey_given_envelope_with_routing_key_will_return_it(t *testing.T) {
	e := WithRoutingKey(envelope.FromMessage("test message"), "test key")

	routingKey := RoutingKey(e)

	assert.Equal(t, "test key", routingKey)
}

func TestRoutingKey_given_envelope_without_routing_key_it_will_return_empty_string(t *testing.T) {
	e := WithRoutingKey(envelope.FromMessage("test message"), "test key")

	routingKey := RoutingKey(e)

	assert.Equal(t, "test key", routingKey)
}

func TestWithoutRoutingKey_will_remove_routing_key_header_on_envelope(t *testing.T) {
	e := envelope.FromMessage("test message")
	eWithRoutingKey := WithRoutingKey(e, "test key")
	eWithoutRoutingKey := WithoutRoutingKey(eWithRoutingKey)

	routingKey, found := eWithoutRoutingKey.LastHeader(routingKeyHeaderName)
	assert.False(t, found)
	assert.Equal(t, "", routingKey)

	assert.True(t, eWithoutRoutingKey.Is(e))
}
