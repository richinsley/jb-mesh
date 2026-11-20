package node

import (
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	"github.com/richinsley/jb-mesh/pkg/replytoken"
)

// A node stops a call only for a cancel its requester could have sent.
func TestNodeStopsACallOnlyForACancelWithItsProof(t *testing.T) {
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	defer ns.Shutdown()
	m, err := mesh.New(mesh.Config{NATSUrl: ns.ClientURL(), NodeName: "n1", Logging: mesh.LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	other, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()

	send := func(msg *nats.Msg) {
		t.Helper()
		if err := other.PublishMsg(msg); err != nil {
			t.Fatal(err)
		}
		if err := other.Flush(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(60 * time.Millisecond)
	}
	// listen starts one call's cancel subscription and returns how many times
	// the call was stopped.
	listen := func(n *Node, callID, token string) *atomic.Int64 {
		t.Helper()
		var stopped atomic.Int64
		sub, err := n.subscribeCancel(callID, token, "tool", "method", "corr-1", func() { stopped.Add(1) })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
		if err := m.Conn().Flush(); err != nil {
			t.Fatal(err)
		}
		return &stopped
	}
	tok, _ := replytoken.New()
	wrong, _ := replytoken.New()

	t.Run("a call that carried a token", func(t *testing.T) {
		n := &Node{mesh: m, nodeName: "n1"}
		stopped := listen(n, "call-1", tok)
		subject := mesh.CancelSubject("call-1")
		// Everything a party that only knows the call id could send.
		send(&nats.Msg{Subject: subject})
		send(&nats.Msg{Subject: subject, Data: []byte("stop")})
		send(&nats.Msg{Subject: subject, Header: nats.Header{replytoken.CancelHeader: []string{"0000"}}})
		send(replytoken.CancelMessage(subject, wrong, "call-1"))
		send(&nats.Msg{Subject: subject, Header: nats.Header{replytoken.CancelHeader: []string{replytoken.CancelProof(tok, "call-2")}}})
		if got := stopped.Load(); got != 0 {
			t.Fatalf("the call was stopped %d time(s) by a cancel without its proof", got)
		}
		if got := n.IgnoredCancels(); got != 5 {
			t.Fatalf("ignored cancels = %d, want 5", got)
		}
		// The requester's own.
		send(replytoken.CancelMessage(subject, tok, "call-1"))
		if got := stopped.Load(); got != 1 {
			t.Fatalf("the requester's cancel stopped the call %d time(s), want 1", got)
		}
		if got := n.IgnoredCancels(); got != 5 {
			t.Fatalf("a valid cancel was counted as ignored (%d)", got)
		}
	})

	t.Run("a call with no token, by default, is cancelled as it always was", func(t *testing.T) {
		n := &Node{mesh: m, nodeName: "n1"}
		stopped := listen(n, "call-legacy", "")
		send(&nats.Msg{Subject: mesh.CancelSubject("call-legacy")})
		if stopped.Load() != 1 || n.IgnoredCancels() != 0 {
			t.Fatalf("stopped=%d ignored=%d", stopped.Load(), n.IgnoredCancels())
		}
	})

	t.Run("a call with no token cannot be cancelled when a proof is required", func(t *testing.T) {
		n := &Node{mesh: m, nodeName: "n1", requireCancelProof: true}
		stopped := listen(n, "call-legacy-2", "")
		subject := mesh.CancelSubject("call-legacy-2")
		send(&nats.Msg{Subject: subject})
		// Nothing can stand in for a proof when there is no token to make one.
		send(replytoken.CancelMessage(subject, tok, "call-legacy-2"))
		send(&nats.Msg{Subject: subject, Header: nats.Header{replytoken.CancelHeader: []string{""}}})
		if got := stopped.Load(); got != 0 {
			t.Fatalf("a tokenless call was stopped %d time(s) with a proof required", got)
		}
		if got := n.IgnoredCancels(); got != 3 {
			t.Fatalf("ignored cancels = %d, want 3", got)
		}
	})

	t.Run("requiring a proof changes nothing for a call that carried a token", func(t *testing.T) {
		n := &Node{mesh: m, nodeName: "n1", requireCancelProof: true}
		stopped := listen(n, "call-3", tok)
		subject := mesh.CancelSubject("call-3")
		send(&nats.Msg{Subject: subject})
		send(replytoken.CancelMessage(subject, tok, "call-3"))
		if stopped.Load() != 1 || n.IgnoredCancels() != 1 {
			t.Fatalf("stopped=%d ignored=%d", stopped.Load(), n.IgnoredCancels())
		}
	})
}
