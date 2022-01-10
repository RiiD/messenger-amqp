package messenger_amqp

import (
	"context"
	"github.com/riid/messenger"
	"github.com/riid/messenger/envelope"
	"github.com/riid/messenger/middleware"
)

type AckFailed struct {
	Envelope messenger.Envelope
	Err      error
	Receiver *receiver
}

func Ack(r *receiver) messenger.Middleware {
	return middleware.Match(r, middleware.HandleFunc(func(ctx context.Context, b messenger.Dispatcher, e messenger.Envelope) messenger.Envelope {
		if !HasAck(e) {
			return e
		}

		err := r.Ack(ctx, e)
		if err != nil {
			b.Dispatch(ctx, envelope.FromMessage(AckFailed{
				Envelope: e,
				Err:      err,
				Receiver: r,
			}))
		}

		return e
	}))
}
