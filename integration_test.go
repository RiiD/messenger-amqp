package messenger_amqp

import (
	"context"
	"fmt"
	"github.com/riid/messenger"
	"github.com/riid/messenger/bridge"
	"github.com/riid/messenger/bus"
	"github.com/riid/messenger/envelope"
	"github.com/riid/messenger/middleware"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"
)

const numberOfMessages = 1028

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGKILL)
	ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	messagesToSend := make([][]byte, numberOfMessages)
	for i := 0; i < len(messagesToSend); i++ {
		messagesToSend[i] = []byte(fmt.Sprintf("message %d", i))
	}

	err := prepareRmq()
	if err != nil {
		t.Fatal("failed to prepare rmq: ", err)
	}

	var senderErr, receiverErr error

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		senderErr = startSender(ctx, messagesToSend)
		wg.Done()
	}()

	r := make(chan messenger.Envelope)
	go func() {
		<-ctx.Done()
		close(r)
	}()

	go func() {
		receiverErr = startReceiver(ctx, r)
		wg.Done()
	}()

	receivedMessages := make([][]byte, numberOfMessages)
	count := 0

	for e := range r {
		id, err := envelope.Int(e, "x-index")
		assert.Nil(t, err)
		receivedMessages[id] = e.Message().([]byte)
		count += 1
		if count == numberOfMessages {
			cancel()
		}
	}

	wg.Wait()

	assert.Same(t, context.Canceled, senderErr)
	assert.Same(t, context.Canceled, receiverErr)
	assert.Equal(t, messagesToSend, receivedMessages)
}

func prepareRmq() error {
	var conn *amqp.Connection
	var err error
	for i := 0; i < 3; i++ {
		conn, err = amqp.Dial("amqp://rabbitmq:5672")
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

	_, err = ch.QueueDeclare("receiver.messages", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	err = ch.QueueBind("receiver.messages", "messages", "sender.messages", false, amqp.Table{})
	if err != nil {
		return err
	}

	return nil
}

func startReceiver(ctx context.Context, received chan messenger.Envelope) error {
	conn, err := amqp.Dial("amqp://rabbitmq:5672")
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
	r := Receiver(ch, ConsumeOptions{
		Queue:       "receiver.messages",
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
			return e
		}),
	), 32, 4)

	br := bridge.New(r, b)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		_ = br.Run(ctx)
		wg.Done()
	}()

	go func() {
		_ = b.Run(ctx)
		wg.Done()
	}()

	wg.Wait()

	return ctx.Err()
}

func startSender(ctx context.Context, messagesToSend [][]byte) error {
	conn, err := amqp.Dial("amqp://rabbitmq:5672")
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
	s := Sender(ch, PublishArgs{
		Exchange:  "sender.messages",
		Mandatory: false,
		Immediate: false,
	})

	b := bus.New(middleware.Stack(
		middleware.Send(s),
	), 32, 4)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i, bytes := range messagesToSend {
			var e messenger.Envelope = envelope.FromMessage(bytes)
			e = envelope.WithInt(e, "x-index", i)
			e = WithRoutingKey(e, "messages")

			b.Dispatch(ctx, e)
		}
		wg.Done()
	}()

	go func() {
		_ = b.Run(ctx)
		wg.Done()
	}()

	wg.Wait()

	return ctx.Err()
}
