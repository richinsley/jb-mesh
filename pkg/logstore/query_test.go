package logstore

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestQueryTailAndStats(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(Config{StorageDir: dir, MaxQueryLimit: 2, MaxQueryWindow: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-30 * time.Minute)
	for i, payload := range []map[string]any{
		{"ts": base.Format(time.RFC3339Nano), "level": "info", "kind": "node", "node": "node-a", "message": "started"},
		{"ts": base.Add(5 * time.Minute).Format(time.RFC3339Nano), "level": "error", "kind": "tool_call", "node": "node-a", "tool": "calc", "method": "sum", "message": "failed", "ok": false},
		{"ts": base.Add(10 * time.Minute).Format(time.RFC3339Nano), "level": "info", "kind": "tool_call", "node": "node-b", "tool": "calc", "method": "sum", "message": "ok", "corr": "corr-1"},
	} {
		b, _ := json.Marshal(payload)
		subject := "logs.node.node-a"
		if i > 0 {
			subject = "logs.call." + payload["node"].(string) + ".calc.sum"
		}
		if _, err := store.Append(context.Background(), subject, b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	resp, err := store.Query(QueryRequest{Since: "1h", Kind: "tool_call", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != 2 || resp.Truncated {
		t.Fatalf("query got len=%d truncated=%v", len(resp.Records), resp.Truncated)
	}
	if resp.Limits.MaxQueryLimit != 2 {
		t.Fatalf("unexpected limits: %+v", resp.Limits)
	}

	tail, err := store.Tail(QueryRequest{Node: "node-b", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(tail.Records) != 1 || tail.Records[0].Node != "node-b" {
		t.Fatalf("tail mismatch: %+v", tail.Records)
	}

	stats, err := store.Stats(StatsRequest{Since: "1h", GroupBy: []string{"node", "kind"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Groups) == 0 {
		t.Fatal("expected stats groups")
	}
}

func TestQueryIncludesLocalDateDirectoryForUTCWindow(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(Config{StorageDir: dir, MaxQueryLimit: 10, MaxQueryWindow: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"ts":      "2026-05-10T18:54:06-07:00",
		"level":   "error",
		"kind":    "tool_call",
		"node":    "node-c",
		"tool":    "evidence-reader",
		"method":  "anomaly_digest",
		"message": "failed",
	})
	if _, err := store.Append(context.Background(), "logs.call.node-c.evidence-reader.anomaly_digest", payload); err != nil {
		t.Fatal(err)
	}

	resp, err := store.Tail(QueryRequest{
		Since: "2026-05-11T01:00:00Z",
		Until: "2026-05-11T02:00:00Z",
		Node:  "node-c",
		Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected local-date record in UTC query window, got %+v", resp.Records)
	}
}

func TestStartQueryServices(t *testing.T) {
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	store, err := NewStore(Config{StorageDir: filepath.Join(t.TempDir(), "logstore"), MaxQueryLimit: 10, MaxQueryWindow: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	payload, _ := json.Marshal(map[string]any{"level": "info", "kind": "node", "node": "node-a", "message": "hi"})
	if _, err := store.Append(context.Background(), "logs.node.node-a", payload); err != nil {
		t.Fatal(err)
	}

	qs, err := store.StartQueryServices(nc)
	if err != nil {
		t.Fatal(err)
	}
	defer qs.Close()

	msg, err := nc.Request("logstore.query", []byte(`{"since":"1h","limit":5}`), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp QueryResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK || len(resp.Records) != 1 {
		t.Fatalf("unexpected resp: %+v", resp)
	}
}
