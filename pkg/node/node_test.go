package node

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	cfgpkg "github.com/richinsley/jb-mesh/pkg/config"
	"github.com/richinsley/jb-mesh/pkg/discovery"
	"github.com/richinsley/jb-mesh/pkg/events"
	"github.com/richinsley/jb-mesh/pkg/logstore"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	"github.com/richinsley/jb-mesh/pkg/state"
	"github.com/richinsley/jb-mesh/pkg/tools"
)

func TestShouldDemote_NewerNodeDemotes(t *testing.T) {
	n := &Node{
		nodeName:  "node-b",
		startedAt: 200,
	}
	other := discovery.Peer{
		Name:      "node-a",
		StartedAt: 100,
	}
	if !n.shouldDemote(other) {
		t.Error("newer node should demote to older seed")
	}
}

func TestShouldDemote_OlderNodeStays(t *testing.T) {
	n := &Node{
		nodeName:  "node-a",
		startedAt: 100,
	}
	other := discovery.Peer{
		Name:      "node-b",
		StartedAt: 200,
	}
	if n.shouldDemote(other) {
		t.Error("older node should NOT demote")
	}
}

func TestShouldDemote_SameTimestamp_AlphabeticalTiebreak(t *testing.T) {
	// Higher name demotes
	n := &Node{
		nodeName:  "macbookpro",
		startedAt: 100,
	}
	other := discovery.Peer{
		Name:      "macbook",
		StartedAt: 100,
	}
	if !n.shouldDemote(other) {
		t.Error("'macbookpro' > 'macbook' alphabetically, should demote")
	}

	// Lower name stays
	n2 := &Node{
		nodeName:  "macbook",
		startedAt: 100,
	}
	other2 := discovery.Peer{
		Name:      "macbookpro",
		StartedAt: 100,
	}
	if n2.shouldDemote(other2) {
		t.Error("'macbook' < 'macbookpro' alphabetically, should NOT demote")
	}
}

func TestOnPeerFound_IgnoresLeafPeers(t *testing.T) {
	n := &Node{
		nodeName:  "seed-node",
		role:      "seed",
		startedAt: 100,
	}

	// Should not panic or trigger demotion for leaf peers
	n.onPeerFound(discovery.Peer{
		Name:      "leaf-node",
		Role:      "leaf",
		StartedAt: 50,
	})

	if n.role != "seed" {
		t.Error("role should not change for leaf peer")
	}
}

func TestOnPeerFound_IgnoresWhenAlreadyLeaf(t *testing.T) {
	n := &Node{
		nodeName:  "leaf-node",
		role:      "leaf",
		startedAt: 200,
	}

	n.onPeerFound(discovery.Peer{
		Name:      "seed-node",
		Role:      "seed",
		StartedAt: 100,
	})

	if n.role != "leaf" {
		t.Error("role should not change when already a leaf")
	}
}

func TestStartLoggingServiceOnlyWhenServerEnabled(t *testing.T) {
	base := t.TempDir()
	cfg := cfgpkg.DefaultConfigWithHome(base)
	cfg.LoggingService.Enabled = true
	cfg.LoggingService.Role = "client"
	n := &Node{cfg: cfg}
	if err := n.startLoggingService(); err != nil {
		t.Fatalf("startLoggingService client role: %v", err)
	}
	if n.logStore != nil || n.logSubscriber != nil || n.logHealthService != nil {
		t.Fatal("logging service should not start for client role")
	}

	cfg2 := cfgpkg.DefaultConfigWithHome(base)
	cfg2.LoggingService.Enabled = false
	cfg2.LoggingService.Role = "server"
	n2 := &Node{cfg: cfg2}
	if err := n2.startLoggingService(); err != nil {
		t.Fatalf("startLoggingService disabled: %v", err)
	}
	if n2.logStore != nil {
		t.Fatal("logging service should not start when disabled")
	}
}

func TestStartLoggingServicePublishesHealthAndWritesLogs(t *testing.T) {
	home := t.TempDir()
	server, err := startEmbeddedNATS(embeddedNATSConfig{StoreDir: filepath.Join(home, "js"), Port: -1, NodeName: "log-node", Role: "seed", LeafPort: -1})
	if err != nil {
		t.Fatalf("startEmbeddedNATS: %v", err)
	}
	defer server.Shutdown()

	m, err := mesh.New(mesh.Config{NATSUrl: server.ClientURL(), NodeName: "log-node"})
	if err != nil {
		t.Fatalf("mesh.New: %v", err)
	}
	defer m.Close()

	cfg := cfgpkg.DefaultConfigWithHome(home)
	cfg.Node.Name = "log-node"
	cfg.LoggingService = cfgpkg.LoggingServiceConfig{
		Enabled:        true,
		Role:           "server",
		StorageDir:     filepath.Join(home, "logstore"),
		Subjects:       []string{"logs.>", "events.>"},
		Redact:         true,
		MaxQueryLimit:  100,
		MaxQueryWindow: "24h",
	}
	mgr := tools.NewManager(cfg)
	n := &Node{cfg: cfg, mesh: m, manager: mgr, nodeName: "log-node"}
	if err := n.startLoggingService(); err != nil {
		t.Fatalf("startLoggingService: %v", err)
	}
	defer n.Close()

	nc, err := nats.Connect(server.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	defer nc.Close()

	payload := []byte(`{"message":"hello","node":"log-node"}`)
	if err := nc.Publish("logs.node.log-node", payload); err != nil {
		t.Fatalf("publish log: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	requireEventuallyFileNode(t, filepath.Join(home, "logstore", "raw", "date="+time.Now().Format("2006-01-02"), "node=log-node.jsonl"))

	msg, err := nc.Request("logstore.health", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("health request: %v", err)
	}
	var health struct {
		OK             bool     `json:"ok"`
		Role           string   `json:"role"`
		Subjects       []string `json:"subjects"`
		RecordsWritten int64    `json:"records_written"`
	}
	if err := json.Unmarshal(msg.Data, &health); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if !health.OK || health.Role != "server" || health.RecordsWritten == 0 {
		t.Fatalf("unexpected health response: %+v", health)
	}

	msg, err = nc.Request("logstore.tail", []byte(`{"since":"1h","limit":5}`), 2*time.Second)
	if err != nil {
		t.Fatalf("tail request: %v", err)
	}
	var tail logstore.QueryResponse
	if err := json.Unmarshal(msg.Data, &tail); err != nil {
		t.Fatalf("unmarshal tail: %v", err)
	}
	if !tail.OK || len(tail.Records) == 0 || tail.Limits.MaxQueryWindow != "24h0m0s" {
		t.Fatalf("unexpected tail response: %+v", tail)
	}

	js, err := m.Conn().JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	stateStore, err := state.NewStore(js)
	if err != nil {
		t.Fatalf("state.NewStore: %v", err)
	}
	nodeHealth, err := stateStore.GetNodeHealth("log-node")
	if err != nil {
		t.Fatalf("GetNodeHealth: %v", err)
	}
	if nodeHealth.Status != "online" {
		t.Fatalf("expected online node health, got %+v", nodeHealth)
	}
}

func TestDemoteToLeafReconnectsMeshToSeedForLogPublishing(t *testing.T) {
	home := t.TempDir()
	seedServer, err := startEmbeddedNATS(embeddedNATSConfig{StoreDir: filepath.Join(home, "seed-js"), Port: -1, NodeName: "seed-node", Role: "seed", LeafPort: -1})
	if err != nil {
		t.Fatalf("start seed nats: %v", err)
	}
	defer seedServer.Shutdown()

	cfg := cfgpkg.DefaultConfigWithHome(home)
	cfg.Node.Name = "leaf-node"
	manager := tools.NewManager(cfg)
	executor := tools.NewExecutor(manager)
	n := &Node{
		cfg:       cfg,
		nodeName:  "leaf-node",
		role:      "seed",
		startedAt: time.Now().Unix(),
		token:     "",
		natsPort:  -1,
		manager:   manager,
		executor:  executor,
	}
	leafLocal, err := startEmbeddedNATS(embeddedNATSConfig{StoreDir: filepath.Join(home, "leaf-js"), Port: -1, NodeName: "leaf-node", Role: "seed", LeafPort: 7423})
	if err != nil {
		t.Fatalf("start leaf local nats: %v", err)
	}
	defer func() {
		if n.natsServer != nil {
			n.natsServer.Shutdown()
		}
	}()
	n.natsServer = leafLocal
	m, err := mesh.New(mesh.Config{NATSUrl: leafLocal.ClientURL(), NodeName: "leaf-node"})
	if err != nil {
		t.Fatalf("leaf local mesh: %v", err)
	}
	defer func() {
		if n.mesh != nil {
			n.mesh.Close()
		}
	}()
	n.mesh = m
	n.logProducer = logstore.NewProducer(m.Conn(), n.nodeName)
	n.eventBus = events.NewBus(m.Conn(), n.nodeName)
	seedLeafPort := DefaultLeafPort
	seedAddr := seedServer.Addr()
	seedPort := seedAddr.(*net.TCPAddr).Port
	n.demoteToLeaf(discovery.Peer{Name: "seed-node", IP: net.ParseIP("127.0.0.1"), Port: seedPort, LeafPort: seedLeafPort, StartedAt: n.startedAt - 10})

	if n.mesh == nil {
		t.Fatal("expected mesh after demotion")
	}
	if got := n.mesh.Conn().ConnectedUrl(); got != fmt.Sprintf("nats://127.0.0.1:%d", seedPort) {
		t.Fatalf("mesh connected url = %q, want %q", got, fmt.Sprintf("nats://127.0.0.1:%d", seedPort))
	}
}

func requireEventuallyFileNode(t *testing.T, path string) {
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
