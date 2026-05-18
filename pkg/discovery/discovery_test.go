package discovery

import (
	"sync"
	"testing"
	"time"
)

func TestNewDiscovery_Defaults(t *testing.T) {
	d, err := NewDiscovery(DiscoveryConfig{
		NodeName: "test-node",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.cfg.NATSPort != DefaultNATSPort {
		t.Errorf("expected default NATS port %d, got %d", DefaultNATSPort, d.cfg.NATSPort)
	}
	if d.cfg.ServiceType != DefaultServiceType {
		t.Errorf("expected service type %q, got %q", DefaultServiceType, d.cfg.ServiceType)
	}
	if d.cfg.Domain != DefaultDomain {
		t.Errorf("expected domain %q, got %q", DefaultDomain, d.cfg.Domain)
	}
}

func TestNewDiscovery_RequiresNodeName(t *testing.T) {
	_, err := NewDiscovery(DiscoveryConfig{})
	if err == nil {
		t.Fatal("expected error for empty NodeName")
	}
}

func TestPeerURLs(t *testing.T) {
	p := Peer{
		Name:     "node-a",
		IP:       []byte{192, 168, 1, 10},
		Port:     4222,
		LeafPort: 7422,
		Role:     "seed",
	}
	if got := p.ClientURL(); got != "nats://192.168.1.10:4222" {
		t.Errorf("ClientURL = %q, want nats://192.168.1.10:4222", got)
	}
	if got := p.LeafURL(); got != "nats-leaf://192.168.1.10:7422" {
		t.Errorf("LeafURL = %q, want nats-leaf://192.168.1.10:7422", got)
	}
}

func TestLeafURL_DefaultPort(t *testing.T) {
	p := Peer{
		Name: "node-b",
		IP:   []byte{192, 168, 1, 20},
		Port: 4222,
		Role: "seed",
		// LeafPort not set → should default to 7422
	}
	if got := p.LeafURL(); got != "nats-leaf://192.168.1.20:7422" {
		t.Errorf("LeafURL = %q, want nats-leaf://192.168.1.20:7422", got)
	}
}

func TestFindSeed(t *testing.T) {
	tests := []struct {
		name     string
		peers    []Peer
		wantName string
		wantNil  bool
	}{
		{
			name:    "no peers",
			peers:   nil,
			wantNil: true,
		},
		{
			name: "no seeds",
			peers: []Peer{
				{Name: "leaf-1", Role: "leaf", StartedAt: 100},
				{Name: "leaf-2", Role: "leaf", StartedAt: 200},
			},
			wantNil: true,
		},
		{
			name: "single seed",
			peers: []Peer{
				{Name: "seed-1", Role: "seed", StartedAt: 100},
				{Name: "leaf-1", Role: "leaf", StartedAt: 200},
			},
			wantName: "seed-1",
		},
		{
			name: "multiple seeds - earliest wins",
			peers: []Peer{
				{Name: "seed-newer", Role: "seed", StartedAt: 200},
				{Name: "seed-older", Role: "seed", StartedAt: 100},
				{Name: "leaf-1", Role: "leaf", StartedAt: 50},
			},
			wantName: "seed-older",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindSeed(tt.peers)
			if tt.wantNil {
				if got != nil {
					t.Errorf("FindSeed() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("FindSeed() = nil, want non-nil")
			}
			if got.Name != tt.wantName {
				t.Errorf("FindSeed().Name = %q, want %q", got.Name, tt.wantName)
			}
		})
	}
}

func TestAdvertiseAndDiscover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mDNS test in short mode")
	}

	// Start node A advertising
	dA, err := NewDiscovery(DiscoveryConfig{
		NodeName:  "test-node-a",
		NATSPort:  14222,
		Version:   "v0.1.0",
		ToolCount: 3,
	})
	if err != nil {
		t.Fatalf("create discovery A: %v", err)
	}

	if err := dA.Start(); err != nil {
		t.Fatalf("start discovery A: %v", err)
	}
	defer dA.Stop()

	// Give advertisement time to register
	time.Sleep(500 * time.Millisecond)

	// Node B browses and should find node A
	dB, err := NewDiscovery(DiscoveryConfig{
		NodeName: "test-node-b",
		NATSPort: 14223,
	})
	if err != nil {
		t.Fatalf("create discovery B: %v", err)
	}

	peers, err := dB.Browse(BrowseTimeout)
	if err != nil {
		t.Fatalf("browse: %v", err)
	}

	var foundA bool
	for _, p := range peers {
		if p.Name == "test-node-a" {
			foundA = true
			if p.Port != 14222 {
				t.Errorf("expected port 14222, got %d", p.Port)
			}
		}
	}
	if !foundA {
		t.Error("node B did not discover node A")
	}
}

func TestTwoDiscoveriesFindEachOther(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mDNS test in short mode")
	}

	var mu sync.Mutex
	foundByA := make(map[string]bool)
	foundByB := make(map[string]bool)

	dA, err := NewDiscovery(DiscoveryConfig{
		NodeName:  "mutual-a",
		NATSPort:  24222,
		Version:   "v0.2.0",
		ToolCount: 1,
	})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	dA.OnPeerFound(func(p Peer) {
		mu.Lock()
		foundByA[p.Name] = true
		mu.Unlock()
	})

	dB, err := NewDiscovery(DiscoveryConfig{
		NodeName:  "mutual-b",
		NATSPort:  24223,
		Version:   "v0.2.0",
		ToolCount: 2,
	})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	dB.OnPeerFound(func(p Peer) {
		mu.Lock()
		foundByB[p.Name] = true
		mu.Unlock()
	})

	if err := dA.Start(); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer dA.Stop()

	if err := dB.Start(); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer dB.Stop()

	// Wait for discovery cycles
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timeout: A found %v, B found %v", foundByA, foundByB)
			mu.Unlock()
			return
		case <-ticker.C:
			mu.Lock()
			aOK := foundByA["mutual-b"]
			bOK := foundByB["mutual-a"]
			mu.Unlock()
			if aOK && bOK {
				return // success
			}
		}
	}
}

func TestPeerCallbackFires(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mDNS test in short mode")
	}

	dA, err := NewDiscovery(DiscoveryConfig{
		NodeName: "callback-advertiser",
		NATSPort: 34222,
	})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	if err := dA.Start(); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer dA.Stop()

	time.Sleep(500 * time.Millisecond)

	found := make(chan Peer, 1)
	dB, err := NewDiscovery(DiscoveryConfig{
		NodeName: "callback-browser",
		NATSPort: 34223,
	})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	dB.OnPeerFound(func(p Peer) {
		if p.Name == "callback-advertiser" {
			select {
			case found <- p:
			default:
			}
		}
	})

	if err := dB.Start(); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer dB.Stop()

	select {
	case p := <-found:
		if p.Port != 34222 {
			t.Errorf("expected port 34222, got %d", p.Port)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for peer callback")
	}
}

func TestStopCleansUp(t *testing.T) {
	d, err := NewDiscovery(DiscoveryConfig{
		NodeName: "cleanup-node",
		NATSPort: 44222,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := d.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Stop should not panic or hang
	done := make(chan struct{})
	go func() {
		d.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("Stop() did not complete within timeout")
	}

	if d.server != nil {
		t.Error("server should be nil after Stop()")
	}
}

func TestEntryToPeer_NilEntry(t *testing.T) {
	if p := entryToPeer(nil); p != nil {
		t.Error("expected nil peer for nil entry")
	}
}

func TestSelfFiltering(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mDNS test in short mode")
	}

	d, err := NewDiscovery(DiscoveryConfig{
		NodeName: "self-filter-node",
		NATSPort: 54222,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := d.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	// Browse should not find ourselves
	peers, err := d.Browse(BrowseTimeout)
	if err != nil {
		t.Fatalf("browse: %v", err)
	}

	for _, p := range peers {
		if p.Name == "self-filter-node" {
			t.Error("should not discover self")
		}
	}
}
