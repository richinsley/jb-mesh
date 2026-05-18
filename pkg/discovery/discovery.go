// Package discovery provides mDNS-based zero-configuration peer discovery
// for jb-mesh nodes on the local network. Nodes advertise themselves as
// _jb-mesh._tcp services and browse for peers to form NATS clusters
// automatically.
package discovery

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
)

const (
	DefaultServiceType = "_jb-mesh._tcp"
	DefaultDomain      = "local."
	DefaultNATSPort    = 4222
	BrowseTimeout      = 5 * time.Second
)

// DiscoveryConfig configures mDNS advertisement and browsing.
type DiscoveryConfig struct {
	NodeName    string
	NATSPort    int    // client port (default 4222)
	LeafPort    int    // leaf connection port (seed only, default 7422)
	Role        string // "seed" or "leaf"
	ServiceType string // default "_jb-mesh._tcp"
	Domain      string // default "local."
	Enabled     bool   // default true
	Version     string // jb-mesh version for TXT records
	ToolCount   int    // number of tools for TXT records
	StartedAt   int64  // unix seconds, for split-brain tiebreaker
}

// Peer represents a discovered jb-mesh node on the network.
type Peer struct {
	Name      string
	Host      string
	Port      int    // NATS client port
	LeafPort  int    // leaf connection port (seed only, 0 for leaves)
	Role      string // "seed" or "leaf"
	IP        net.IP
	Version   string
	ToolCount int
	StartedAt int64 // unix seconds
}

// LeafURL returns the NATS leaf connection URL for this peer.
// Only meaningful for seed peers.
func (p Peer) LeafURL() string {
	port := p.LeafPort
	if port == 0 {
		port = 7422
	}
	return fmt.Sprintf("nats-leaf://%s:%d", p.IP, port)
}

// ClientURL returns the NATS client URL for this peer.
func (p Peer) ClientURL() string {
	return fmt.Sprintf("nats://%s:%d", p.IP, p.Port)
}

// FindSeed returns the best seed peer from a list of peers.
// If multiple seeds exist, returns the one with the earliest StartedAt
// (tiebreaker for split-brain). Returns nil if no seeds found.
func FindSeed(peers []Peer) *Peer {
	var best *Peer
	for i := range peers {
		if peers[i].Role != "seed" {
			continue
		}
		if best == nil || peers[i].StartedAt < best.StartedAt {
			best = &peers[i]
		}
	}
	return best
}

// Discovery manages mDNS service advertisement and browsing for jb-mesh peers.
type Discovery struct {
	cfg DiscoveryConfig

	mu      sync.RWMutex
	peers   map[string]Peer // keyed by node name
	onFound []func(Peer)
	onLost  []func(Peer)

	server     *zeroconf.Server
	cancel     context.CancelFunc
	browseCtx  context.Context
	browseDone chan struct{}
}

// NewDiscovery creates a new Discovery instance with the given configuration.
func NewDiscovery(cfg DiscoveryConfig) (*Discovery, error) {
	if cfg.NodeName == "" {
		return nil, fmt.Errorf("discovery: NodeName is required")
	}
	if cfg.NATSPort == 0 {
		cfg.NATSPort = DefaultNATSPort
	}
	if cfg.ServiceType == "" {
		cfg.ServiceType = DefaultServiceType
	}
	if cfg.Domain == "" {
		cfg.Domain = DefaultDomain
	}

	return &Discovery{
		cfg:   cfg,
		peers: make(map[string]Peer),
	}, nil
}

// Start begins mDNS advertisement and background browsing for peers.
func (d *Discovery) Start() error {
	if err := d.startAdvertising(); err != nil {
		return fmt.Errorf("discovery: failed to advertise: %w", err)
	}

	d.browseCtx, d.cancel = context.WithCancel(context.Background())
	d.browseDone = make(chan struct{})

	go d.browseLoop()

	return nil
}

// Browse performs a one-shot mDNS browse with a timeout. Returns discovered peers.
// This is intended to be called before Start() to find existing peers for
// initial cluster formation.
func (d *Discovery) Browse(timeout time.Duration) ([]Peer, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, fmt.Errorf("discovery: failed to create resolver: %w", err)
	}

	entries := make(chan *zeroconf.ServiceEntry)
	var peers []Peer
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		defer close(done)
		for entry := range entries {
			peer := entryToPeer(entry)
			if peer == nil || peer.Name == d.cfg.NodeName {
				continue
			}
			mu.Lock()
			peers = append(peers, *peer)
			mu.Unlock()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := resolver.Browse(ctx, d.cfg.ServiceType, d.cfg.Domain, entries); err != nil {
		return nil, fmt.Errorf("discovery: browse failed: %w", err)
	}

	<-ctx.Done()
	<-done // wait for all entries to be processed after channel close

	mu.Lock()
	result := make([]Peer, len(peers))
	copy(result, peers)
	mu.Unlock()
	return result, nil
}

// Stop shuts down mDNS advertisement and browsing.
func (d *Discovery) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
	if d.browseDone != nil {
		<-d.browseDone
	}
	if d.server != nil {
		d.server.Shutdown()
		d.server = nil
	}
}

// Peers returns a snapshot of currently known peers.
func (d *Discovery) Peers() []Peer {
	d.mu.RLock()
	defer d.mu.RUnlock()

	peers := make([]Peer, 0, len(d.peers))
	for _, p := range d.peers {
		peers = append(peers, p)
	}
	return peers
}

// OnPeerFound registers a callback that fires when a new peer is discovered.
func (d *Discovery) OnPeerFound(fn func(Peer)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onFound = append(d.onFound, fn)
}

// OnPeerLost registers a callback that fires when a peer disappears.
func (d *Discovery) OnPeerLost(fn func(Peer)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onLost = append(d.onLost, fn)
}

func (d *Discovery) startAdvertising() error {
	txt := []string{
		"node=" + d.cfg.NodeName,
		"nats-port=" + strconv.Itoa(d.cfg.NATSPort),
	}
	if d.cfg.Role != "" {
		txt = append(txt, "role="+d.cfg.Role)
	}
	if d.cfg.Role == "seed" && d.cfg.LeafPort > 0 {
		txt = append(txt, "leaf-port="+strconv.Itoa(d.cfg.LeafPort))
	}
	if d.cfg.Version != "" {
		txt = append(txt, "version="+d.cfg.Version)
	}
	if d.cfg.StartedAt > 0 {
		txt = append(txt, "started="+strconv.FormatInt(d.cfg.StartedAt, 10))
	}
	txt = append(txt, "tools="+strconv.Itoa(d.cfg.ToolCount))

	server, err := zeroconf.Register(
		d.cfg.NodeName,    // instance name
		d.cfg.ServiceType, // service type
		d.cfg.Domain,      // domain
		d.cfg.NATSPort,    // port
		txt,               // TXT records
		nil,               // interfaces (nil = all)
	)
	if err != nil {
		return err
	}

	d.server = server
	log.Printf("[discovery] advertising %s on %s port %d", d.cfg.NodeName, d.cfg.ServiceType, d.cfg.NATSPort)
	return nil
}

func (d *Discovery) browseLoop() {
	defer close(d.browseDone)

	for {
		d.browseOnce()

		select {
		case <-d.browseCtx.Done():
			return
		case <-time.After(10 * time.Second):
			// Re-browse periodically to detect new peers and lost peers
		}
	}
}

func (d *Discovery) browseOnce() {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		log.Printf("[discovery] failed to create resolver: %v", err)
		return
	}

	entries := make(chan *zeroconf.ServiceEntry)
	seen := make(map[string]bool)
	var mu sync.Mutex

	go func() {
		for entry := range entries {
			peer := entryToPeer(entry)
			if peer == nil || peer.Name == d.cfg.NodeName {
				continue
			}

			mu.Lock()
			seen[peer.Name] = true
			mu.Unlock()

			d.mu.Lock()
			_, existed := d.peers[peer.Name]
			d.peers[peer.Name] = *peer
			callbacks := make([]func(Peer), len(d.onFound))
			copy(callbacks, d.onFound)
			d.mu.Unlock()

			if !existed {
				log.Printf("[discovery] found peer %s at %s:%d", peer.Name, peer.IP, peer.Port)
				for _, fn := range callbacks {
					fn(*peer)
				}
			}
		}
	}()

	ctx, cancel := context.WithTimeout(d.browseCtx, BrowseTimeout)
	defer cancel()

	if err := resolver.Browse(ctx, d.cfg.ServiceType, d.cfg.Domain, entries); err != nil {
		log.Printf("[discovery] browse error: %v", err)
		return
	}

	<-ctx.Done()
	if d.browseCtx.Err() != nil {
		return // shutting down
	}

	// Detect lost peers
	mu.Lock()
	seenCopy := seen
	mu.Unlock()

	d.mu.Lock()
	var lostPeers []Peer
	for name, peer := range d.peers {
		if !seenCopy[name] {
			lostPeers = append(lostPeers, peer)
			delete(d.peers, name)
		}
	}
	lostCallbacks := make([]func(Peer), len(d.onLost))
	copy(lostCallbacks, d.onLost)
	d.mu.Unlock()

	for _, peer := range lostPeers {
		log.Printf("[discovery] lost peer %s", peer.Name)
		for _, fn := range lostCallbacks {
			fn(peer)
		}
	}
}

// entryToPeer converts a zeroconf service entry into a Peer.
func entryToPeer(entry *zeroconf.ServiceEntry) *Peer {
	if entry == nil {
		return nil
	}

	peer := &Peer{
		Name: entry.Instance,
		Host: entry.HostName,
		Port: entry.Port,
	}

	// Prefer IPv4
	if len(entry.AddrIPv4) > 0 {
		peer.IP = entry.AddrIPv4[0]
	} else if len(entry.AddrIPv6) > 0 {
		peer.IP = entry.AddrIPv6[0]
	}

	// Parse TXT records (key=value format)
	for _, txt := range entry.Text {
		key, val, ok := strings.Cut(txt, "=")
		if !ok {
			continue
		}
		switch key {
		case "nats-port":
			if v, err := strconv.Atoi(val); err == nil {
				peer.Port = v
			}
		case "role":
			peer.Role = val
		case "leaf-port":
			if v, err := strconv.Atoi(val); err == nil {
				peer.LeafPort = v
			}
		case "started":
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				peer.StartedAt = v
			}
		case "version":
			peer.Version = val
		case "tools":
			if v, err := strconv.Atoi(val); err == nil {
				peer.ToolCount = v
			}
		case "node":
			// node name from TXT (informational, instance name is canonical)
		}
	}

	return peer
}
