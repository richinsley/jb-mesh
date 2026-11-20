package replytoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"

	"github.com/nats-io/nats.go"
)

// A call is cancelled by a message on a subject named for its call id. Anyone
// who may publish there and knows the id could send one. So a cancel carries
// a proof that its sender knows the call's reply token, which only the
// requester and the node it called hold. The token itself is never put on the
// cancel subject, which many parties can read: the proof is a keyed hash that
// is good for this one call's cancellation and nothing else.

// CancelHeader is the header that carries a cancel's proof.
const CancelHeader = "Jb-Cancel-Proof"

// cancelDomain separates this use of the token from any other.
const cancelDomain = "jb-mesh/cancel/v1"

// CancelProof is HMAC-SHA256 keyed with the call's reply token over the
// domain label and the call id, hex-encoded. It is "" when either is empty:
// a call with no token has no proof.
func CancelProof(token, callID string) string {
	if token == "" || callID == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(cancelDomain))
	mac.Write([]byte{0})
	mac.Write([]byte(callID))
	return hex.EncodeToString(mac.Sum(nil))
}

// CancelMessage is the message a requester publishes to cancel a call: empty,
// on subject, carrying the proof for token and callID when there is one.
func CancelMessage(subject, token, callID string) *nats.Msg {
	msg := &nats.Msg{Subject: subject}
	if proof := CancelProof(token, callID); proof != "" {
		msg.Header = nats.Header{CancelHeader: []string{proof}}
	}
	return msg
}

// ValidCancel reports whether msg carries the proof for this call. It is
// false when the call has no token: there is then nothing to check a cancel
// against.
func ValidCancel(msg *nats.Msg, token, callID string) bool {
	want := CancelProof(token, callID)
	if want == "" || msg == nil || msg.Header == nil {
		return false
	}
	return hmac.Equal([]byte(msg.Header.Get(CancelHeader)), []byte(want))
}
