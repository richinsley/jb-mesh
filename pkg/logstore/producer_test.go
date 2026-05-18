package logstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestSubjectFor(t *testing.T) {
	if got := SubjectFor("node-a", "node", "", ""); got != "logs.node.node-a" {
		t.Fatalf("node subject = %q", got)
	}
	if got := SubjectFor("node-a", "tool", "calc", ""); got != "logs.tool.node-a.calc" {
		t.Fatalf("tool subject = %q", got)
	}
	if got := SubjectFor("node-a", "tool_call", "calc", "sum"); got != "logs.call.node-a.calc.sum" {
		t.Fatalf("call subject = %q", got)
	}
}

func TestProducerPublishIncludesStructuredFields(t *testing.T) {
	ns, err := server.NewServer(&server.Options{Port: -1})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	ch := make(chan *nats.Msg, 1)
	if _, err := nc.Subscribe("logs.call.node-a.calc.sum", func(msg *nats.Msg) { ch <- msg }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush sub: %v", err)
	}

	p := NewProducer(nc, "node-a")
	if err := p.Publish(PublishOptions{Level: "info", Kind: "tool_call", Tool: "calc", Method: "sum", Corr: "corr-123", Duration: 25 * time.Millisecond, OK: BoolPtr(true), Message: "calc.sum completed", Data: map[string]any{"response_bytes": 12}}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg := <-ch:
		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if payload["corr"] != "corr-123" || payload["tool"] != "calc" || payload["method"] != "sum" {
			t.Fatalf("unexpected payload: %#v", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for publish")
	}
}
