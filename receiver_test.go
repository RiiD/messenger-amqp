package messenger_amqp

import (
	"context"
	"errors"
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"math"
	"strconv"
	"testing"
	"time"
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

	assert.NoError(t, err)
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

	assert.NoError(t, err)
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

func TestReceiver_Receive(t *testing.T) {
	dd := make(chan amqp.Delivery, 1)

	now := time.Now()

	dd <- amqp.Delivery{
		Headers: amqp.Table{
			"X-Custom-String": "test value",

			"X-Custom-Int":   math.MinInt,
			"X-Custom-Int8":  math.MinInt8,
			"X-Custom-Int16": math.MinInt16,
			"X-Custom-Int32": math.MinInt32,
			"X-Custom-Int64": math.MinInt64,

			"X-Custom-Float32": float32(math.MaxFloat32),
			"X-Custom-Float64": math.MaxFloat64,

			"X-Custom-String-Array": []string{"test value1", "test value2"},
			"X-Custom-Int-Array":    []int{math.MinInt, math.MaxInt},
			"X-Custom-Float-Array":  []float64{math.MaxFloat64},
		},
		ContentType:     "test content type",
		ContentEncoding: "test content encoding",
		CorrelationId:   "test correlation id",
		ReplyTo:         "test reply to",
		Expiration:      "test expiration",
		MessageId:       "test message id",
		Timestamp:       now,
		Type:            "test message type",
		UserId:          "test user id",
		AppId:           "test app id",
		ConsumerTag:     "test consumer tag",
		DeliveryTag:     123,
	}

	close(dd)

	ch := &mockChannel{}
	ch.On("Consume", "test-queue", "test-consumer", false, false, false, false, amqp.Table(nil)).Return(dd, nil)
	ch.On("Cancel", "test-consumer", false).Return(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	receiver := Receiver(ch, ConsumeOptions{
		Queue:       "test-queue",
		ConsumerTag: "test-consumer",
	}, "test-alias")

	rch, err := receiver.Receive(ctx)

	assert.NoError(t, err)

	e := <-rch

	customString := e.Header("X-Custom-String")
	assert.Equal(t, []string{"test value"}, customString)

	customInt := e.Header("X-Custom-Int")
	assert.Equal(t, []string{strconv.FormatInt(math.MinInt, 10)}, customInt)
	customInt8 := e.Header("X-Custom-Int8")
	assert.Equal(t, []string{strconv.FormatInt(math.MinInt8, 10)}, customInt8)
	customInt16 := e.Header("X-Custom-Int16")
	assert.Equal(t, []string{strconv.FormatInt(math.MinInt16, 10)}, customInt16)
	customInt32 := e.Header("X-Custom-Int32")
	assert.Equal(t, []string{strconv.FormatInt(math.MinInt32, 10)}, customInt32)
	customInt64 := e.Header("X-Custom-Int64")
	assert.Equal(t, []string{strconv.FormatInt(math.MinInt64, 10)}, customInt64)

	customFloat32, err := envelope.Float32(e, "X-Custom-Float32")
	assert.NoError(t, err)
	assert.Equal(t, float32(math.MaxFloat32), customFloat32)
	customFloat64, err := envelope.Float64(e, "X-Custom-Float64")
	assert.NoError(t, err)
	assert.Equal(t, math.MaxFloat64, customFloat64)

	customStringArray := e.Header("X-Custom-String-Array")
	assert.Equal(t, []string{"test value1", "test value2"}, customStringArray)

	customIntArray := e.Header("X-Custom-Int-Array")
	assert.Equal(t, []string{strconv.FormatInt(math.MinInt, 10), strconv.FormatInt(math.MaxInt, 10)}, customIntArray)

	customFloatArray := e.Header("X-Custom-Float-Array")
	assert.Equal(t, []string{
		strconv.FormatFloat(math.MaxFloat64, 'g', -1, 64),
	}, customFloatArray)

	alias, tag, err := received(e)
	assert.Equal(t, "test-alias", alias)
	assert.Equal(t, uint64(123), tag)
	assert.NoError(t, err)

	ct := envelope.ContentType(e)
	assert.Equal(t, "test content type", ct)

	cid := envelope.CorrelationID(e)
	assert.Equal(t, "test correlation id", cid)

	rt := envelope.ReplyTo(e)
	assert.Equal(t, "test reply to", rt)

	mid := envelope.ID(e)
	assert.Equal(t, "test message id", mid)

	ts, err := envelope.Timestamp(e)

	assert.True(t, ts.Equal(now))
	assert.NoError(t, err)

	uid := envelope.UserID(e)
	assert.Equal(t, "test user id", uid)

	aid := envelope.AppID(e)
	assert.Equal(t, "test app id", aid)

	mt := envelope.MessageType(e)
	assert.Equal(t, "test message type", mt)
}
