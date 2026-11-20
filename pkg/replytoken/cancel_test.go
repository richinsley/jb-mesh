package replytoken

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestCancelProofIsBoundToTheTokenAndTheCall(t *testing.T) {
	tok, _ := New()
	other, _ := New()
	p := CancelProof(tok, "call-1")
	if len(p) != 64 || strings.Trim(p, "0123456789abcdef") != "" {
		t.Fatalf("proof %q is not 64 lower-case hex characters", p)
	}
	if CancelProof(tok, "call-1") != p {
		t.Fatal("the proof is not deterministic")
	}
	for name, q := range map[string]string{
		"another call":  CancelProof(tok, "call-2"),
		"another token": CancelProof(other, "call-1"),
	} {
		if q == p {
			t.Fatalf("%s has the same proof", name)
		}
	}
	// It is not the token, nor a plain hash of it, nor contains it.
	plain := sha256.Sum256([]byte(tok))
	if p == tok || p == hex.EncodeToString(plain[:]) || strings.Contains(p, tok) {
		t.Fatal("the proof exposes the token or is a bare hash of it")
	}
	// No token or no call id: no proof.
	if CancelProof("", "call-1") != "" || CancelProof(tok, "") != "" {
		t.Fatal("a proof was made with no token or no call id")
	}
}

func TestValidCancelIsExact(t *testing.T) {
	tok, _ := New()
	other, _ := New()
	good := CancelMessage("cancel.call-1", tok, "call-1")
	if good.Subject != "cancel.call-1" || len(good.Data) != 0 || !ValidCancel(good, tok, "call-1") {
		t.Fatalf("the requester's own cancel is not valid: %+v", good)
	}
	// The cancel never carries the token itself.
	for k, vs := range good.Header {
		for _, v := range vs {
			if strings.Contains(v, tok) {
				t.Fatalf("header %s carries the token", k)
			}
		}
	}
	with := func(v string) *nats.Msg { return &nats.Msg{Header: nats.Header{CancelHeader: []string{v}}} }
	for name, msg := range map[string]*nats.Msg{
		"nil":                             nil,
		"bare, as an old client sends":    {Subject: "cancel.call-1"},
		"empty proof":                     with(""),
		"the token as the proof":          with(tok),
		"a proof for another call":        with(CancelProof(tok, "call-2")),
		"a proof made with another token": with(CancelProof(other, "call-1")),
		"a truncated proof":               with(CancelProof(tok, "call-1")[:63]),
		"an upper-case proof":             with(strings.ToUpper(CancelProof(tok, "call-1"))),
		"the reply token header":          {Header: nats.Header{Header: []string{tok}}},
	} {
		if ValidCancel(msg, tok, "call-1") {
			t.Fatalf("%s was accepted", name)
		}
	}
	// A call with no token accepts no cancel as valid, including a bare one.
	if ValidCancel(&nats.Msg{}, "", "call-1") || ValidCancel(with(""), "", "call-1") {
		t.Fatal("a cancel was valid for a call with no token")
	}
	// A message built for a call with no token carries no proof header.
	if m := CancelMessage("cancel.x", "", "x"); m.Header != nil {
		t.Fatalf("a cancel for a tokenless call carries a header: %v", m.Header)
	}
}
