// Package replytoken binds a reply to the request it answers.
//
// A NATS publisher chooses the reply subject of the message it sends, and the
// server does not check that the publisher owns that subject. So anyone who
// may send a request to a responder can aim the response at somebody else's
// reply subject, and a requester cannot tell from the subject alone whether a
// message answers its request. Controlling who may publish to a reply subject
// does not help: the responder is honest and allowed.
//
// A requester therefore sends a fresh random token with each request, in a
// header. A responder copies the header onto everything it publishes for that
// request. The requester accepts only messages that carry the token. The
// token is known to the requester and to whoever the server delivered the
// request to, and to nobody else unless one of them discloses it or the
// connection can be read.
//
// What that proves is possession of the token. It does not prove which
// process produced a reply.
package replytoken

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"

	"github.com/nats-io/nats.go"
)

// Header is the message header that carries the token, on a request and on
// every message published in answer to it.
const Header = "Jb-Reply-Token"

// Size is the number of random bytes in a token. It is hex-encoded on the
// wire, so a token is 2*Size characters.
const Size = 32

// New returns a fresh token from the system's cryptographic random source.
func New() (string, error) {
	var b [Size]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("reply token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// Of returns the token a message carries, or "" if it carries none.
func Of(msg *nats.Msg) string {
	if msg == nil || msg.Header == nil {
		return ""
	}
	return msg.Header.Get(Header)
}

// Matches reports whether msg carries exactly token. An empty token matches
// nothing. The comparison does not depend on where the values differ.
func Matches(msg *nats.Msg, token string) bool {
	got := Of(msg)
	if token == "" || len(got) != len(token) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

// Echo returns the header a responder puts on what it publishes in answer to
// req: the request's token, or nil when the request carried none.
func Echo(req *nats.Msg) nats.Header {
	token := Of(req)
	if token == "" {
		return nil
	}
	return nats.Header{Header: []string{token}}
}

// EchoToken is Echo for a responder that holds the token and not the message.
func EchoToken(token string) nats.Header {
	if token == "" {
		return nil
	}
	return nats.Header{Header: []string{token}}
}

// Respond answers req with data, carrying req's token. It is msg.Respond for
// a responder: every reply a node publishes goes through it or through
// Publish, so no reply path can forget the token.
func Respond(req *nats.Msg, data []byte) error {
	if req == nil {
		return nats.ErrMsgNotBound
	}
	return req.RespondMsg(&nats.Msg{Data: data, Header: Echo(req)})
}

// Publish sends data to req's reply subject on nc, carrying req's token. It
// is for a responder that publishes more than one message for a request, such
// as the frames of a stream.
func Publish(nc *nats.Conn, req *nats.Msg, data []byte) error {
	if req == nil || req.Reply == "" {
		return nats.ErrMsgNoReply
	}
	return nc.PublishMsg(&nats.Msg{Subject: req.Reply, Data: data, Header: Echo(req)})
}
