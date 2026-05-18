package logstore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestNormalizeValidationAndInference(t *testing.T) {
	cfg := Config{Redact: true}
	payload := []byte(`{"level":"weird","message":"hello","data":{"token":"abc"}}`)
	rec, err := Normalize("logs.call.node-b.example-tool.health", payload, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Schema != SchemaV1 || rec.Kind != "tool_call" || rec.Node != "node-b" || rec.Tool != "example-tool" || rec.Method != "health" {
		t.Fatalf("unexpected normalized record: %+v", rec)
	}
	if rec.Level != "info" {
		t.Fatalf("expected level info, got %s", rec.Level)
	}
	if rec.Corr == "" {
		t.Fatal("expected corr to be generated")
	}
	if rec.Data["token"] != "[REDACTED]" {
		t.Fatalf("expected token redaction, got %#v", rec.Data["token"])
	}
}

func TestRecursiveRedaction(t *testing.T) {
	cfg := Config{Redact: true}
	payload := []byte(`{"message":"x","node":"macbook","data":{"nested":{"apiKey":"secret"},"list":[{"authorization":"Bearer x"}]}}`)
	rec, err := Normalize("logs.node.macbook", payload, cfg)
	if err != nil {
		t.Fatal(err)
	}
	nested := rec.Data["nested"].(map[string]any)
	if nested["apiKey"] != "[REDACTED]" {
		t.Fatalf("nested redaction failed: %#v", nested)
	}
	list := rec.Data["list"].([]any)
	first := list[0].(map[string]any)
	if first["authorization"] != "[REDACTED]" {
		t.Fatalf("slice redaction failed: %#v", first)
	}
}

func TestSizeCapsAndTruncationMarkers(t *testing.T) {
	cfg := Config{Redact: true, MessageMaxBytes: 8, DataMaxBytes: 40, RecordMaxBytes: 120}
	payload := []byte(`{"message":"123456789999","node":"macbook","data":{"blob":"` + strings.Repeat("x", 200) + `"}}`)
	rec, err := Normalize("logs.node.macbook", payload, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Message != "12345678" || !rec.Truncated {
		t.Fatalf("expected truncated message, got %+v", rec)
	}
	if rec.Data["truncated"] != true {
		t.Fatalf("expected data truncation marker, got %#v", rec.Data)
	}
	dir := t.TempDir()
	store, err := NewStore(Config{StorageDir: dir, RecordMaxBytes: 160, Redact: true})
	if err != nil {
		t.Fatal(err)
	}
	bigPayload, _ := json.Marshal(map[string]any{"message": strings.Repeat("m", 300), "node": "macbook", "data": map[string]any{"x": strings.Repeat("y", 300)}})
	stored, err := store.Append(context.Background(), "logs.node.macbook", bigPayload)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Message != "record truncated" || stored.Data["truncated"] != true {
		t.Fatalf("expected whole-record truncation marker, got %+v", stored)
	}
}

func TestStoragePathSanitizationAndRotation(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(Config{StorageDir: dir, Redact: true})
	if err != nil {
		t.Fatal(err)
	}
	payload1 := []byte(`{"ts":"2026-05-09T23:59:59Z","node":"../worker/node","message":"one"}`)
	payload2 := []byte(`{"ts":"2026-05-10T00:00:01Z","node":"../worker/node","message":"two"}`)
	if _, err := store.Append(context.Background(), "logs.node.bad", payload1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), "logs.node.bad", payload2); err != nil {
		t.Fatal(err)
	}
	p1 := filepath.Join(dir, "raw", "date=2026-05-09", "node=worker-node.jsonl")
	p2 := filepath.Join(dir, "raw", "date=2026-05-10", "node=worker-node.jsonl")
	if _, err := os.Stat(p1); err != nil {
		t.Fatalf("expected first rotated file: %v", err)
	}
	if _, err := os.Stat(p2); err != nil {
		t.Fatalf("expected second rotated file: %v", err)
	}
}

func TestEventNormalizationWhenEnabled(t *testing.T) {
	dir := t.TempDir()
	s, nc := runNATSServer(t)
	defer s.Shutdown()
	defer nc.Close()
	store, err := NewStore(Config{StorageDir: dir, Redact: true, CaptureEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := Subscribe(nc, store)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	payload := map[string]any{"type": "tool.started", "node": "node-b", "timestamp": "2026-05-09T13:51:00Z", "corr": "corr-123", "data": map[string]any{"tool": "example-tool", "api_key": "secret"}}
	b, _ := json.Marshal(payload)
	if err := nc.Publish("events.tool.started", b); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	requireEventuallyFile(t, filepath.Join(dir, "raw", "date=2026-05-09", "node=node-b.jsonl"))
	content, err := os.ReadFile(filepath.Join(dir, "raw", "date=2026-05-09", "node=node-b.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var rec Record
	line := strings.TrimSpace(string(content))
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Kind != "event" || rec.Subject != "events.tool.started" || rec.Message != "tool.started" {
		t.Fatalf("unexpected event record: %+v", rec)
	}
	if rec.Corr != "corr-123" {
		t.Fatalf("expected corr preservation, got %q", rec.Corr)
	}
	if rec.Data["api_key"] != "[REDACTED]" {
		t.Fatalf("expected event redaction, got %#v", rec.Data["api_key"])
	}
}

func TestSubscriberCloseCleansUpSubscriptionsAndStore(t *testing.T) {
	dir := t.TempDir()
	s, nc := runNATSServer(t)
	defer s.Shutdown()
	defer nc.Close()
	store, err := NewStore(Config{StorageDir: dir, Redact: true, CaptureEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := Subscribe(nc, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
}

func requireEventuallyFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for file %s", path)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runNATSServer(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, JetStream: false}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	return s, nc
}
