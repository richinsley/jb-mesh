package events

import (
	"fmt"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func startTestNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	ns := natsserver.RunServer(&opts)
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestBus_EmitAndSubscribe(t *testing.T) {
	nc := startTestNATS(t)
	bus := NewBus(nc, "test-node")

	var received Event
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := bus.Subscribe("events.>", func(e Event) {
		received = e
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Small delay to let subscription propagate
	time.Sleep(50 * time.Millisecond)

	event := ToolInstalled("test-node", "whisper", "v1.0.0")
	if err := bus.Emit(event); err != nil {
		t.Fatalf("emit: %v", err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Type != "tool.installed" {
		t.Fatalf("expected tool.installed, got %s", received.Type)
	}
	if received.Node != "test-node" {
		t.Fatalf("expected test-node, got %s", received.Node)
	}
	if received.Data["tool"] != "whisper" {
		t.Fatalf("expected whisper, got %v", received.Data["tool"])
	}
	if received.Data["version"] != "v1.0.0" {
		t.Fatalf("expected v1.0.0, got %v", received.Data["version"])
	}
	if received.Timestamp.IsZero() {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestBus_SubscribeType(t *testing.T) {
	nc := startTestNATS(t)
	bus := NewBus(nc, "node-a")

	var crashReceived Event
	var mu sync.Mutex
	otherCount := 0

	// Subscribe only to crashes
	bus.SubscribeType("tool.crashed", func(e Event) {
		mu.Lock()
		crashReceived = e
		mu.Unlock()
	})

	// Subscribe to installs (to verify filtering)
	bus.SubscribeType("tool.installed", func(e Event) {
		mu.Lock()
		otherCount++
		mu.Unlock()
	})

	time.Sleep(50 * time.Millisecond)

	// Emit multiple event types
	bus.Emit(ToolStarted("node-a", "embed"))
	bus.Emit(ToolCrashed("node-a", "whisper", fmt.Errorf("OOM killed"), 2))
	bus.Emit(ToolInstalled("node-a", "calc", "v1.0.0"))

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if crashReceived.Type != "tool.crashed" {
		t.Fatalf("expected tool.crashed, got %s", crashReceived.Type)
	}
	if crashReceived.Data["error"] != "OOM killed" {
		t.Fatalf("expected OOM killed, got %v", crashReceived.Data["error"])
	}
	// restart_count comes back as float64 from JSON
	if crashReceived.Data["restart_count"] != float64(2) {
		t.Fatalf("expected restart_count 2, got %v", crashReceived.Data["restart_count"])
	}
	if otherCount != 1 {
		t.Fatalf("expected 1 install event, got %d", otherCount)
	}
}

func TestBus_WildcardSubscribe(t *testing.T) {
	nc := startTestNATS(t)
	bus := NewBus(nc, "node-a")

	var count int
	var mu sync.Mutex

	// Subscribe to all tool events
	bus.Subscribe("events.tool.*", func(e Event) {
		mu.Lock()
		count++
		mu.Unlock()
	})

	time.Sleep(50 * time.Millisecond)

	bus.Emit(ToolInstalled("node-a", "whisper", "v1.0.0"))
	bus.Emit(ToolStarted("node-a", "whisper"))
	bus.Emit(ToolStopped("node-a", "whisper", "user"))
	bus.Emit(NodeJoined("node-b", nil)) // should NOT match events.tool.*

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 3 {
		t.Fatalf("expected 3 tool events, got %d", count)
	}
}

func TestBus_NodeEvents(t *testing.T) {
	nc := startTestNATS(t)
	bus := NewBus(nc, "node-a")

	var received []Event
	var mu sync.Mutex

	bus.Subscribe("events.node.*", func(e Event) {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
	})

	time.Sleep(50 * time.Millisecond)

	bus.Emit(NodeJoined("node-a", map[string]interface{}{
		"gpu": true, "vram_gb": 24,
	}))
	bus.Emit(NodeHealth("node-a", "online", map[string]interface{}{
		"gpu_util": 0.45,
	}))
	bus.Emit(NodeLeft("node-a", "shutdown"))

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("expected 3 node events, got %d", len(received))
	}
	if received[0].Type != "node.joined" {
		t.Fatalf("expected node.joined, got %s", received[0].Type)
	}
	if received[2].Type != "node.left" {
		t.Fatalf("expected node.left, got %s", received[2].Type)
	}
}

func TestBus_UserEvents(t *testing.T) {
	nc := startTestNATS(t)
	bus := NewBus(nc, "node-a")

	var received Event
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe to all user events
	bus.Subscribe("events.user.>", func(e Event) {
		received = e
		wg.Done()
	})

	time.Sleep(50 * time.Millisecond)

	event := UserEvent("node-a", "training.complete", map[string]interface{}{
		"model":  "my-fine-tune",
		"epochs": 100,
		"loss":   0.023,
	})
	bus.Emit(event)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	if received.Type != "user.training.complete" {
		t.Fatalf("expected user.training.complete, got %s", received.Type)
	}
	if received.Data["model"] != "my-fine-tune" {
		t.Fatalf("expected my-fine-tune, got %v", received.Data["model"])
	}
}

func TestBus_DefaultNode(t *testing.T) {
	nc := startTestNATS(t)
	bus := NewBus(nc, "default-node")

	var received Event
	var wg sync.WaitGroup
	wg.Add(1)

	bus.Subscribe("events.>", func(e Event) {
		received = e
		wg.Done()
	})

	time.Sleep(50 * time.Millisecond)

	// Emit with empty node — should fill in default
	event := Event{Type: "tool.started", Data: map[string]interface{}{"tool": "test"}}
	bus.Emit(event)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	if received.Node != "default-node" {
		t.Fatalf("expected default-node, got %s", received.Node)
	}
}

func startTestJetStream(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natsserver.RunServer(&opts)
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return nc, js
}

func TestPersistentBus_History(t *testing.T) {
	nc, js := startTestJetStream(t)
	bus, err := NewPersistentBus(nc, js, "node-a", DefaultPersistConfig())
	if err != nil {
		t.Fatalf("NewPersistentBus: %v", err)
	}

	// Emit several events
	bus.Emit(ToolInstalled("node-a", "whisper", "v1.0.0"))
	bus.Emit(ToolStarted("node-a", "whisper"))
	bus.Emit(ToolCrashed("node-a", "whisper", fmt.Errorf("oom"), 1))

	// Small delay for JetStream to persist
	time.Sleep(200 * time.Millisecond)

	// Query history
	history, err := bus.History("events.>", 100)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 events, got %d", len(history))
	}
	if history[0].Type != "tool.installed" {
		t.Fatalf("expected tool.installed first, got %s", history[0].Type)
	}
	if history[2].Type != "tool.crashed" {
		t.Fatalf("expected tool.crashed last, got %s", history[2].Type)
	}
}

func TestPersistentBus_FilteredHistory(t *testing.T) {
	nc, js := startTestJetStream(t)
	bus, err := NewPersistentBus(nc, js, "node-a", DefaultPersistConfig())
	if err != nil {
		t.Fatal(err)
	}

	bus.Emit(ToolInstalled("node-a", "whisper", "v1.0.0"))
	bus.Emit(NodeJoined("node-b", nil))
	bus.Emit(ToolStarted("node-a", "whisper"))
	bus.Emit(NodeHealth("node-a", "online", nil))

	time.Sleep(200 * time.Millisecond)

	// Only tool events
	toolEvents, err := bus.History("events.tool.*", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(toolEvents) != 2 {
		t.Fatalf("expected 2 tool events, got %d", len(toolEvents))
	}

	// Only node events
	nodeEvents, err := bus.History("events.node.*", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodeEvents) != 2 {
		t.Fatalf("expected 2 node events, got %d", len(nodeEvents))
	}
}

func TestPersistentBus_HistorySince(t *testing.T) {
	nc, js := startTestJetStream(t)
	bus, err := NewPersistentBus(nc, js, "node-a", DefaultPersistConfig())
	if err != nil {
		t.Fatal(err)
	}

	bus.Emit(ToolInstalled("node-a", "old-tool", "v1.0.0"))
	time.Sleep(100 * time.Millisecond)

	cutoff := time.Now()
	time.Sleep(100 * time.Millisecond)

	bus.Emit(ToolInstalled("node-a", "new-tool", "v2.0.0"))
	time.Sleep(200 * time.Millisecond)

	// Only events after cutoff
	events, err := bus.HistorySince("events.>", cutoff, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after cutoff, got %d", len(events))
	}
	if events[0].Data["tool"] != "new-tool" {
		t.Fatalf("expected new-tool, got %v", events[0].Data["tool"])
	}
}

func TestEventConstructors(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		wantType string
	}{
		{"installed", ToolInstalled("n", "t", "v1.0.0"), "tool.installed"},
		{"configured", ToolConfigured("n", "t", nil), "tool.configured"},
		{"started", ToolStarted("n", "t"), "tool.started"},
		{"stopped", ToolStopped("n", "t", "user"), "tool.stopped"},
		{"crashed", ToolCrashed("n", "t", fmt.Errorf("oom"), 1), "tool.crashed"},
		{"removed", ToolRemoved("n", "t"), "tool.removed"},
		{"joined", NodeJoined("n", nil), "node.joined"},
		{"left", NodeLeft("n", "shutdown"), "node.left"},
		{"health", NodeHealth("n", "online", nil), "node.health"},
		{"user", UserEvent("n", "custom.topic", nil), "user.custom.topic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.event.Type != tt.wantType {
				t.Fatalf("expected type %s, got %s", tt.wantType, tt.event.Type)
			}
			if tt.event.Node != "n" {
				t.Fatalf("expected node 'n', got %s", tt.event.Node)
			}
		})
	}
}
