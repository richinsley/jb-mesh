package state

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func startJetStreamServer(t *testing.T) (*server.Server, nats.JetStreamContext) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1 // random port
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

	return ns, js
}

func TestNewStore(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, err := NewStore(js)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestToolState_SetGet(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, err := NewStore(js)
	if err != nil {
		t.Fatal(err)
	}

	state := ToolState{
		Status:      "idle",
		Node:        "node-a",
		Tool:        "whisper",
		Version:     "v1.0.0",
		ModelLoaded: "large-v3",
		VRAMUsedGB:  4.2,
	}

	if err := store.SetToolState(state); err != nil {
		t.Fatalf("SetToolState: %v", err)
	}

	got, err := store.GetToolState("node-a", "whisper")
	if err != nil {
		t.Fatalf("GetToolState: %v", err)
	}
	if got.Status != "idle" {
		t.Fatalf("expected idle, got %s", got.Status)
	}
	if got.ModelLoaded != "large-v3" {
		t.Fatalf("expected large-v3, got %s", got.ModelLoaded)
	}
	if got.VRAMUsedGB != 4.2 {
		t.Fatalf("expected 4.2, got %f", got.VRAMUsedGB)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero UpdatedAt")
	}
}

func TestToolState_Update(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	// Set initial
	store.SetToolState(ToolState{
		Status: "idle",
		Node:   "node-a",
		Tool:   "embed",
	})

	// Update to busy
	store.SetToolState(ToolState{
		Status:      "busy",
		Node:        "node-a",
		Tool:        "embed",
		CurrentTask: "indexing",
	})

	got, _ := store.GetToolState("node-a", "embed")
	if got.Status != "busy" {
		t.Fatalf("expected busy, got %s", got.Status)
	}
	if got.CurrentTask != "indexing" {
		t.Fatalf("expected indexing, got %s", got.CurrentTask)
	}
}

func TestToolState_List(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	store.SetToolState(ToolState{Status: "idle", Node: "node-a", Tool: "whisper"})
	store.SetToolState(ToolState{Status: "busy", Node: "node-b", Tool: "embed"})
	store.SetToolState(ToolState{Status: "idle", Node: "node-a", Tool: "calc"})

	states, err := store.ListToolStates()
	if err != nil {
		t.Fatalf("ListToolStates: %v", err)
	}
	if len(states) != 3 {
		t.Fatalf("expected 3 states, got %d", len(states))
	}
}

func TestToolState_Delete(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	store.SetToolState(ToolState{Status: "idle", Node: "node-a", Tool: "whisper"})

	if err := store.DeleteToolState("node-a", "whisper"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.GetToolState("node-a", "whisper")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestToolState_ListEmpty(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	states, err := store.ListToolStates()
	if err != nil {
		t.Fatalf("ListToolStates: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("expected 0, got %d", len(states))
	}
}

func TestToolState_ExtraFields(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	store.SetToolState(ToolState{
		Status: "idle",
		Node:   "node-a",
		Tool:   "custom",
		Extra: map[string]interface{}{
			"temperature": 0.7,
			"top_k":       40,
		},
	})

	got, _ := store.GetToolState("node-a", "custom")
	if got.Extra["temperature"] != 0.7 {
		t.Fatalf("expected 0.7, got %v", got.Extra["temperature"])
	}
}

// --- Node Health ---

func TestNodeHealth_SetGet(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	health := NodeHealth{
		Node:     "node-a",
		Status:   "online",
		GPU:      true,
		GPUModel: "RTX 3090",
		VRAMGB:   24,
		CPUCores: 20,
		MemoryGB: 64,
		Arch:     "amd64",
		OS:       "linux",
	}

	if err := store.SetNodeHealth(health); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}

	got, err := store.GetNodeHealth("node-a")
	if err != nil {
		t.Fatalf("GetNodeHealth: %v", err)
	}
	if got.Status != "online" {
		t.Fatalf("expected online, got %s", got.Status)
	}
	if !got.GPU {
		t.Fatal("expected GPU=true")
	}
	if got.VRAMGB != 24 {
		t.Fatalf("expected 24GB VRAM, got %d", got.VRAMGB)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero UpdatedAt")
	}
}

func TestNodeHealth_List(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	store.SetNodeHealth(NodeHealth{Node: "node-a", Status: "online", GPU: true})
	store.SetNodeHealth(NodeHealth{Node: "macbook", Status: "online"})
	store.SetNodeHealth(NodeHealth{Node: "rpi", Status: "degraded"})

	nodes, err := store.ListNodeHealth()
	if err != nil {
		t.Fatalf("ListNodeHealth: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
}

func TestNodeHealth_Delete(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	store.SetNodeHealth(NodeHealth{Node: "temp-node", Status: "online"})
	store.DeleteNodeHealth("temp-node")

	_, err := store.GetNodeHealth("temp-node")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestNodeHealth_WithRunningTools(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	store.SetNodeHealth(NodeHealth{
		Node:         "node-a",
		Status:       "online",
		ToolCount:    3,
		RunningTools: []string{"whisper", "embed", "z-image"},
	})

	got, _ := store.GetNodeHealth("node-a")
	if got.ToolCount != 3 {
		t.Fatalf("expected 3 tools, got %d", got.ToolCount)
	}
	if len(got.RunningTools) != 3 {
		t.Fatalf("expected 3 running tools, got %d", len(got.RunningTools))
	}
}

func TestToolState_Watch(t *testing.T) {
	_, js := startJetStreamServer(t)
	store, _ := NewStore(js)

	watcher, err := store.WatchToolStates()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer watcher.Stop()

	// Write a state after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		store.SetToolState(ToolState{Status: "busy", Node: "node-a", Tool: "whisper"})
	}()

	// Should receive the update (first entry may be nil = end of initial values)
	timeout := time.After(2 * time.Second)
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				continue // skip initial-values example-tool
			}
			var state ToolState
			if err := json.Unmarshal(entry.Value(), &state); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if state.Status != "busy" {
				t.Fatalf("expected busy, got %s", state.Status)
			}
			return // success
		case <-timeout:
			t.Fatal("timeout waiting for watch update")
		}
	}
}
