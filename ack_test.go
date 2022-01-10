package messenger_amqp

import (
	"context"
	"errors"
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/riid/messenger/mock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAck_given_receiver_and_matching_envelope_when_ack_called_then_it_should_call_receiver_ack(t *testing.T) {
	ctx := context.Background()
	b := &mock.Dispatcher{}

	ch := &mockChannel{}
	ch.On("Ack", uint64(123), false).Return(nil)

	r := Receiver(ch, ConsumeOptions{}, "test-receiver")

	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-receiver", 123)
	e = WithAck(e)

	m := Ack(r)

	nextCalled := false
	m.Handle(ctx, b, e, func(nctx context.Context, ne messenger.Envelope) {
		nextCalled = true
		assert.Same(t, ctx, nctx)
		assert.Same(t, e, ne)
	})

	assert.True(t, nextCalled)
}

func TestAck_given_receiver_and_not_matching_envelope_when_ack_called_then_it_should_not_call_receiver_ack(t *testing.T) {
	ctx := context.Background()
	b := &mock.Dispatcher{}

	ch := &mockChannel{}

	r := Receiver(ch, ConsumeOptions{}, "test-receiver")

	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "other-receiver", 123)
	e = WithAck(e)

	m := Ack(r)

	nextCalled := false
	m.Handle(ctx, b, e, func(nctx context.Context, ne messenger.Envelope) {
		nextCalled = true
		assert.Same(t, ctx, nctx)
		assert.Same(t, e, ne)
	})

	assert.True(t, nextCalled)
}

func TestAck_given_receiver_and_matching_envelope_when_receiver_ack_returns_error_then_it_should_dispatch_ack_failed_event(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("test error")

	ch := &mockChannel{}
	ch.On("Ack", uint64(123), false).Return(expectedErr)

	r := Receiver(ch, ConsumeOptions{}, "test-receiver")

	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-receiver", 123)
	e = WithAck(e)

	expectedEvent := AckFailed{
		Envelope: e,
		Err:      expectedErr,
		Receiver: r,
	}

	b := &mock.Dispatcher{}
	b.On("Dispatch", ctx, envelope.FromMessage(expectedEvent))

	m := Ack(r)

	nextCalled := false
	m.Handle(ctx, b, e, func(nctx context.Context, ne messenger.Envelope) {
		nextCalled = true
		assert.Same(t, ctx, nctx)
		assert.Same(t, e, ne)
	})

	b.AssertCalled(t, "Dispatch", ctx, envelope.FromMessage(expectedEvent))
	assert.True(t, nextCalled)
}

func TestAck_given_matching_but_not_acked_envelope_when_ack_called_it_should_ignore_the_envelope(t *testing.T) {
	ctx := context.Background()

	ch := &mockChannel{}

	r := Receiver(ch, ConsumeOptions{}, "test-receiver")

	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-receiver", 123)

	b := &mock.Dispatcher{}

	m := Ack(r)

	nextCalled := false
	m.Handle(ctx, b, e, func(nctx context.Context, ne messenger.Envelope) {
		nextCalled = true
		assert.Same(t, ctx, nctx)
		assert.Same(t, e, ne)
	})

	assert.True(t, nextCalled)
}
