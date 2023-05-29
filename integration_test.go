package messenger_amqp

import (
	"context"
	"fmt"
	"log"
	"math"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/riid/messenger"
	"github.com/riid/messenger/bridge"
	"github.com/riid/messenger/bus"
	"github.com/riid/messenger/envelope"
	"github.com/riid/messenger/matcher"
	"github.com/riid/messenger/middleware"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

const numberOfMessages = 1028
const amqpURL = "amqp://rabbitmq:5672"

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGKILL)
	ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	messagesToSend := make([]messenger.Envelope, numberOfMessages)
	for i := 0; i < len(messagesToSend); i++ {
		var e messenger.Envelope = envelope.FromMessage([]byte(fmt.Sprintf("message %d", i)))
		e = envelope.WithInt(e, "x-index", i)
		e = WithRoutingKey(e, "receiver.messages")

		messagesToSend[i] = e
	}

	err := prepareRmq("receiver.messages")
	if err != nil {
		t.Fatal("failed to prepare rmq: ", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = startSender(ctx, messagesToSend)
	}()

	r := make(chan messenger.Envelope)
	go func() {
		<-ctx.Done()
		close(r)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = startReceiver(ctx, "receiver.messages", r)
	}()

	receivedMessages := make([]messenger.Envelope, numberOfMessages)
	count := 0

	for e := range r {
		id, err := envelope.Int(e, "x-index")
		assert.NoError(t, err)
		receivedMessages[id] = e

		count += 1
		if count == numberOfMessages {
			cancel()
		}
	}

	wg.Wait()

	for i := 0; i < numberOfMessages; i++ {
		toSend := WithoutRoutingKey(messagesToSend[i])
		var received messenger.Envelope = envelope.WithoutHeader(receivedMessages[i], deliveryTagHeader)
		received = envelope.WithoutHeader(received, receiverAliasHeaderName)
		received = WithoutRoutingKey(received)

		assert.Equal(t, toSend.Message(), received.Message())
		assert.Equal(t, toSend.Headers(), received.Headers())
	}
}

func TestIntegration_header_types(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	err := prepareRmq("receiver.types")
	if err != nil {
		t.Fatal("failed to prepare rmq: ", err)
	}

	var e messenger.Envelope = envelope.FromMessage([]byte("test message"))

	e = envelope.WithInt(e, "int", math.MinInt)
	e = envelope.WithInt8(e, "int8", math.MinInt8)
	e = envelope.WithInt16(e, "int16", math.MinInt16)
	e = envelope.WithInt32(e, "int32", math.MinInt32)
	e = envelope.WithInt64(e, "int64", math.MinInt64)

	e = envelope.WithUint(e, "uint", math.MaxUint)
	e = envelope.WithUint8(e, "uint8", math.MaxUint8)
	e = envelope.WithUint16(e, "uint16", math.MaxUint16)
	e = envelope.WithUint32(e, "uint32", math.MaxUint32)
	e = envelope.WithUint64(e, "uint64", math.MaxUint64)

	e = envelope.WithFloat32(e, "float32", math.MaxFloat32)
	e = envelope.WithFloat64(e, "float64", math.MaxFloat64)

	e = envelope.WithHeader(e, "string", "string value")

	e = WithRoutingKey(e, "receiver.types")

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGKILL)
	ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	wg.Add(2)
	go func() {
		_ = startSender(ctx, []messenger.Envelope{e})
		log.Println("sender stopped")
		wg.Done()
	}()

	res := make(chan messenger.Envelope, 1)
	go func() {
		_ = startReceiver(ctx, "receiver.types", res)
		log.Println("receiver stopped")
		wg.Done()
	}()

	resE := <-res
	log.Println("got message")

	intValue, err := envelope.Int(resE, "int")
	assert.NoError(t, err)
	assert.Equal(t, math.MinInt, intValue)

	int8Value, err := envelope.Int8(resE, "int8")
	assert.NoError(t, err)
	assert.Equal(t, int8(math.MinInt8), int8Value)

	int16Value, err := envelope.Int16(resE, "int16")
	assert.NoError(t, err)
	assert.Equal(t, int16(math.MinInt16), int16Value)

	int32Value, err := envelope.Int32(resE, "int32")
	assert.NoError(t, err)
	assert.Equal(t, int32(math.MinInt32), int32Value)

	int64Value, err := envelope.Int64(resE, "int64")
	assert.NoError(t, err)
	assert.Equal(t, int64(math.MinInt64), int64Value)

	uintValue, err := envelope.Uint(resE, "uint")
	assert.NoError(t, err)
	assert.Equal(t, uint(math.MaxUint), uintValue)

	uint8Value, err := envelope.Uint8(resE, "uint8")
	assert.NoError(t, err)
	assert.Equal(t, uint8(math.MaxUint8), uint8Value)

	uint16Value, err := envelope.Uint16(resE, "uint16")
	assert.NoError(t, err)
	assert.Equal(t, uint16(math.MaxUint16), uint16Value)

	uint32Value, err := envelope.Uint32(resE, "uint32")
	assert.NoError(t, err)
	assert.Equal(t, uint32(math.MaxUint32), uint32Value)

	uint64Value, err := envelope.Uint64(resE, "uint64")
	assert.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), uint64Value)

	float32Value, err := envelope.Float32(resE, "float32")
	assert.NoError(t, err)
	assert.Equal(t, float32(math.MaxFloat32), float32Value)

	float64Value, err := envelope.Float64(resE, "float64")
	assert.NoError(t, err)
	assert.Equal(t, math.MaxFloat64, float64Value)

	log.Println("got to end")
}

func prepareRmq(q string) error {
	var conn *amqp.Connection
	var err error
	for i := 0; i < 3; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err != nil {
			<-time.After(3 * time.Second)
			continue
		}
		break
	}
	if err != nil {
		return err
	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	err = ch.ExchangeDeclare("sender.messages", "topic", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(q, false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	_, err = ch.QueuePurge(q, false)
	if err != nil {
		return err
	}

	err = ch.QueueBind(q, q, "sender.messages", false, amqp.Table{})
	if err != nil {
		return err
	}

	return nil
}

func startReceiver(ctx context.Context, q string, received chan messenger.Envelope) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
		log.Println("receiver amqp conn closed")
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
		log.Println("receiver amqp chan closed")
	}(ch)
	r := Receiver(ch, ConsumeOptions{
		Queue:       q,
		ConsumerTag: "test-receiver",
		AutoAck:     false,
		Exclusive:   false,
		NoLocal:     false,
		NoWait:      false,
		Args:        nil,
	}, "test-receiver")

	b := bus.New(middleware.Stack(
		middleware.HandleFunc(func(ctx context.Context, b messenger.Dispatcher, e messenger.Envelope) messenger.Envelope {
			received <- e
			return envelope.WithAck(e)
		}),
		middleware.Ack(r, r),
	), 32, 4)

	br := bridge.New(r, b)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = br.Run(ctx)
		log.Println("receiver bridge done")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.Run(ctx)
		log.Println("receiver bus done")
	}()

	return ctx.Err()
}

func startSender(ctx context.Context, messagesToSend []messenger.Envelope) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
		log.Println("sender amqp conn closed")
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
		log.Println("sender amqp chan closed")
	}(ch)
	s := Sender(ch, PublishArgs{
		Exchange:  "sender.messages",
		Mandatory: false,
		Immediate: false,
	})

	b := bus.New(middleware.Match(
		matcher.Type([]byte{}),
		middleware.Send(s),
	), len(messagesToSend)+1, 4)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, e := range messagesToSend {
			b.Dispatch(ctx, e)
		}
		log.Println("sent all messages")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.Run(ctx)
		log.Println("sender bus done")
	}()

	return ctx.Err()
}
