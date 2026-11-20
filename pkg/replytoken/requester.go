package replytoken

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// Requester sends requests on one connection and decides which messages on a
// reply subject are answers.
//
// With Require false it behaves as a plain NATS request that also sends a
// token: the first message wins. With Require true a message is an answer
// only if it carries the request's token; anything else that arrives on the
// reply subject is dropped and counted, and the request goes on waiting.
type Requester struct {
	NC *nats.Conn
	// Require makes the token mandatory on every reply. Nothing without the
	// request's token ends a request then, including the server's own "no
	// responders" status: the server cannot put a token on it, and anyone who
	// may publish to the reply subject can imitate it. A request to a subject
	// nobody serves therefore ends at its deadline, not at once.
	Require bool
	// OnReject, if set, is called for each rejected message with the request's
	// subject and why it was rejected. It must not block. It is never given
	// a token.
	OnReject func(subject, reason string)

	rejected atomic.Uint64
}

// Rejections is how many messages this requester has dropped for not carrying
// the token of the request whose reply subject they arrived on.
func (r *Requester) Rejections() uint64 { return r.rejected.Load() }

// RejectedError reports a request that ended without a valid reply while
// messages that did not carry its token were arriving on its reply subject.
// It says what was seen. It does not say why the sender lacked the token.
// A request to a subject nobody serves ends this way too, with the server's
// status counted among the rejected.
type RejectedError struct {
	Subject  string
	Rejected int
	// Err is how the request ended: the context's error, or nats.ErrTimeout
	// from the timeout form.
	Err error
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("request to %s: %v; %d message(s) arrived on its reply subject without this request's reply token and were rejected", e.Subject, e.Err, e.Rejected)
}

func (e *RejectedError) Unwrap() error { return e.Err }

func (r *Requester) reject(subject, reason string) {
	r.rejected.Add(1)
	if r.OnReject != nil {
		r.OnReject(subject, reason)
	}
}

// isNoResponders reports the status message the server sends when a request's
// subject has no subscriber. The server writes it, so it never carries a token.
func isNoResponders(msg *nats.Msg) bool {
	return len(msg.Data) == 0 && msg.Header != nil && msg.Header.Get("Status") == "503"
}

// classify says why a message is not an answer to the request with token.
func classify(msg *nats.Msg) string {
	if isNoResponders(msg) {
		return "a no-responders status, which cannot carry a reply token"
	}
	if Of(msg) == "" {
		return "no reply token"
	}
	return "another request's reply token"
}

// Request sends data to subject and returns the answer. ctx bounds the wait.
func (r *Requester) Request(ctx context.Context, subject string, data []byte) (*nats.Msg, error) {
	token, err := New()
	if err != nil {
		return nil, err
	}
	req := &nats.Msg{Subject: subject, Data: data, Header: EchoToken(token)}
	if !r.Require {
		return r.NC.RequestMsgWithContext(ctx, req)
	}
	// A verifying request reads its own reply subscription. The connection's
	// shared one hands back the first message for a reply subject, and the
	// first message is exactly what a forger can supply.
	req.Reply = r.NC.NewRespInbox()
	ch := make(chan *nats.Msg, 16)
	sub, err := r.NC.ChanSubscribe(req.Reply, ch)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err := r.NC.PublishMsg(req); err != nil {
		return nil, err
	}
	rejected := 0
	for {
		select {
		case msg := <-ch:
			if Matches(msg, token) {
				return msg, nil
			}
			rejected++
			r.reject(subject, classify(msg))
		case <-ctx.Done():
			if rejected > 0 {
				return nil, &RejectedError{Subject: subject, Rejected: rejected, Err: ctx.Err()}
			}
			return nil, ctx.Err()
		}
	}
}

// Stream is a streaming request in progress. C delivers each accepted answer
// and is closed when the stream ends.
type Stream struct {
	C        <-chan *nats.Msg
	rejected atomic.Int64
}

// Rejected is how many messages on this stream's reply subject were dropped
// for not carrying its token. It is final once C is closed.
func (s *Stream) Rejected() int { return int(s.rejected.Load()) }

// OpenStream sends data to subject and delivers the answers on the returned
// stream until ctx ends or done, called for each delivered message, reports
// the last one.
//
// With Require set, only messages carrying the request's token are delivered,
// so a message injected between two frames is dropped and the stream goes on,
// and nothing but a token-carrying message or ctx ends it. Without it every
// message on the reply subject is delivered, as before.
func (r *Requester) OpenStream(ctx context.Context, subject string, data []byte, buffer int, done func(*nats.Msg) bool) (*Stream, error) {
	token, err := New()
	if err != nil {
		return nil, err
	}
	req := &nats.Msg{Subject: subject, Data: data, Header: EchoToken(token), Reply: r.NC.NewRespInbox()}
	if buffer < 1 {
		buffer = 1
	}
	raw := make(chan *nats.Msg, buffer)
	sub, err := r.NC.ChanSubscribe(req.Reply, raw)
	if err != nil {
		return nil, err
	}
	if err := r.NC.PublishMsg(req); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	out := make(chan *nats.Msg, 1)
	s := &Stream{C: out}
	go func() {
		defer close(out)
		defer sub.Unsubscribe()
		for {
			select {
			case msg := <-raw:
				if r.Require && !Matches(msg, token) {
					s.rejected.Add(1)
					r.reject(subject, classify(msg))
					continue
				}
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
				if done(msg) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return s, nil
}

// IsNoResponders reports whether msg is the server's "nobody is subscribed"
// status and not an answer.
func IsNoResponders(msg *nats.Msg) bool { return msg != nil && isNoResponders(msg) }

// AsRejected returns the RejectedError in err's chain, if there is one.
func AsRejected(err error) (*RejectedError, bool) {
	var re *RejectedError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}
