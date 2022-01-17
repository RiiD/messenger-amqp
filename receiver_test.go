package messenger_amqp

import (
	"context"
	"errors"
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReceiver_Matches(t *testing.T) {
	ch := &mockChannel{}
	r := Receiver(ch, ConsumeOptions{}, "test-alias")

	type testCase struct {
		e              messenger.Envelope
		expectedResult bool
	}

	tcc := map[string]testCase{
		"when called on envelope with same receiver alias should return true": {
			e: withReceived(
				envelope.FromMessage("test message"),
				"test-alias",
				123,
			),
			expectedResult: true,
		},
		"when called on envelope with different receiver alias should return false": {
			e: withReceived(
				envelope.FromMessage("test message"),
				"other-alias",
				123,
			),
			expectedResult: false,
		},
		"when called on envelope without receiver alias should return false": {
			e:              envelope.FromMessage("test message"),
			expectedResult: false,
		},
	}

	for name, tc := range tcc {
		t.Run(name, func(t *testing.T) {
			res := r.Matches(tc.e)
			assert.Equal(t, tc.expectedResult, res)
		})
	}
}

func TestReceiver_Ack_when_called_on_envelope_with_wrong_receiver_alias_should_return_invalid_alias_error(t *testing.T) {
	ch := &mockChannel{}
	r := Receiver(ch, ConsumeOptions{}, "test-alias")
	ctx := context.Background()
	e := withReceived(envelope.FromMessage("test message"), "other-alias", 123)

	err := r.Ack(ctx, e)

	assert.Same(t, ErrInvalidAlias, err)
}

func TestReceiver_Ack_when_called_on_matching_envelope_with_acked_header_should_call_channel_ack(t *testing.T) {
	ch := &mockChannel{}
	ch.On("Ack", uint64(123), false).Return(nil)
	r := Receiver(ch, ConsumeOptions{}, "test-alias")
	ctx := context.Background()
	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-alias", 123)

	err := r.Ack(ctx, e)

	assert.Nil(t, err)
	ch.AssertCalled(t, "Ack", uint64(123), false)
}

func TestReceiver_Ack_channel_ack_returns_error_it_should_return_the_same_error(t *testing.T) {
	expectedErr := errors.New("test error")

	ch := &mockChannel{}
	ch.On("Ack", uint64(123), false).Return(expectedErr)
	r := Receiver(ch, ConsumeOptions{}, "test-alias")
	ctx := context.Background()
	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-alias", 123)

	err := r.Ack(ctx, e)

	assert.Same(t, expectedErr, err)
}

func TestReceiver_Nack_when_called_on_envelope_with_wrong_receiver_alias_should_return_invalid_alias_error(t *testing.T) {
	ch := &mockChannel{}
	r := Receiver(ch, ConsumeOptions{}, "test-alias")
	ctx := context.Background()
	e := withReceived(envelope.FromMessage("test message"), "other-alias", 123)

	err := r.Nack(ctx, e)

	assert.Same(t, ErrInvalidAlias, err)
}

func TestReceiver_Nack_when_called_on_matching_envelope_with_nack_header_should_call_channel_nack(t *testing.T) {
	ch := &mockChannel{}
	ch.On("Nack", uint64(123), false, false).Return(nil)
	r := Receiver(ch, ConsumeOptions{}, "test-alias")
	ctx := context.Background()
	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-alias", 123)

	err := r.Nack(ctx, e)

	assert.Nil(t, err)
	ch.AssertCalled(t, "Nack", uint64(123), false, false)
}

func TestReceiver_Nack_channel_ack_returns_error_it_should_return_the_same_error(t *testing.T) {
	expectedErr := errors.New("test error")

	ch := &mockChannel{}
	ch.On("Nack", uint64(123), false, false).Return(expectedErr)
	r := Receiver(ch, ConsumeOptions{}, "test-alias")
	ctx := context.Background()
	var e messenger.Envelope = envelope.FromMessage("test message")
	e = withReceived(e, "test-alias", 123)

	err := r.Nack(ctx, e)

	assert.Same(t, expectedErr, err)
}
