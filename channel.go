package messenger_amqp

import (
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/mock"
)

type Channel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
}

type mockChannel struct {
	mock.Mock
}

func (m *mockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return m.Called(exchange, key, mandatory, immediate, msg).Error(0)
}

func (m *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	called := m.Called(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if ch, ok := called.Get(0).(<-chan amqp.Delivery); ok {
		return ch, called.Error(1)
	}
	return nil, called.Error(1)
}

func (m *mockChannel) Cancel(consumer string, noWait bool) error {
	return m.Called(consumer, noWait).Error(0)
}

func (m *mockChannel) Ack(tag uint64, multiple bool) error {
	return m.Called(tag, multiple).Error(0)
}

func (m *mockChannel) Nack(tag uint64, multiple bool, requeue bool) error {
	return m.Called(tag, multiple, requeue).Error(0)
}
