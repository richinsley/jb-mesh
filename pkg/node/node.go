// Package node manages the lifecycle of a jb-mesh node.
//
// A node runs tools locally via jumpboot and registers them into the NATS mesh.
// Optionally, a node can run an embedded NATS server so no external NATS is needed.
package node

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/config"
	"github.com/richinsley/jb-mesh/pkg/discovery"
	"github.com/richinsley/jb-mesh/pkg/events"
	"github.com/richinsley/jb-mesh/pkg/filestore"
	"github.com/richinsley/jb-mesh/pkg/logstore"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	"github.com/richinsley/jb-mesh/pkg/state"
	"github.com/richinsley/jb-mesh/pkg/tools"
)

// Node represents a single jb-mesh node
type Node struct {
	cfg              *config.Config
	mesh             *mesh.Mesh
	manager          *tools.Manager
	executor         *tools.Executor
	eventBus         *events.Bus // event emission (never nil after New)
	logStore         *logstore.Store
	logSubscriber    *logstore.Subscriber
	logHealthService *logstore.HealthService
	logQueryService  *logstore.QueryService
	logStateStore    *state.Store
	logProducer      *logstore.Producer
	log              nodeLog
	logging          LoggingConfig
	natsServer       *natsserver.Server   // non-nil if embedded
	discovery        *discovery.Discovery // non-nil if mDNS enabled
	nodeName         string
	role             string     // "seed" or "leaf" (empty if pure client mode)
	startedAt        int64      // unix seconds, for split-brain tiebreaker
	token            string     // saved for demotion reconnect
	natsPort         int        // saved for demotion reconnect
	natsHost         string     // saved for demotion reconnect
	leafPort         int        // saved for demotion reconnect
	leafHost         string     // saved for demotion reconnect
	mu               sync.Mutex // protects role transitions (demotion)

	// Per-tool raw streaming subscriptions (`.stream` subjects). These are
	// NOT tracked by mesh.Mesh because they bypass NATS micro; the Node owns
	// their lifecycle directly and cleans them up on re-register/uninstall.
	streamSubsMu sync.Mutex
	streamSubs   map[string][]*nats.Subscription
}

// Config holds node startup configuration
type Config struct {
	NATSUrl       string // explicit NATS URL (skips embed + mDNS)
	NodeName      string
	Token         string
	Authorization Authorization
	NATSWebSocket mesh.NATSWebSocketConfig
	HomeDir       string
	NoMDNS        bool   // disable mDNS discovery
	Role          string // "seed", "leaf", or "" (auto-detect via mDNS)
	LeafURL       string // explicit leaf connection URL (--leaf flag)
	LeafPort      int    // embedded NATS leaf-node bind port for seed role
	EmbedHost     string // embedded NATS client bind host; empty preserves jb-mesh default (0.0.0.0)
	LeafHost      string // embedded NATS leaf-node bind host for seed role; empty preserves jb-mesh default (0.0.0.0)
	WebsocketHost string // embedded NATS websocket bind host; empty disables websocket
	WebsocketPort int    // embedded NATS websocket bind port
	Logging       LoggingConfig

	// EmbedPort overrides the embedded NATS client port. If zero, the value
	// from <HomeDir>/jb-mesh.yaml is used, falling back to DefaultNATSPort
	// (4222). Pass -1 to bind a random free port — useful for parallel
	// tests or running multiple in-process nodes on one host.
	EmbedPort int
}

// Authorization is the narrow typed authorization seam for embedded NATS seeds.
// It is deliberately NOT a raw NATS config escape hatch: callers provide named
// principals with credential material and allowed subject subtrees; node translates
// that into NATS users/permissions. When Enabled is true, the embedded seed refuses
// global token auth and denies every external principal not listed here.
type Authorization struct {
	Enabled    bool
	Principals []AuthorizedPrincipal

	// InternalUsername/InternalPassword are optional process-local credentials for
	// this node's own jb-mesh client connection to its embedded server. If either
	// is empty, node.New generates both in memory. They are never logged.
	InternalUsername string
	InternalPassword string
}

// AuthorizedPrincipal is one external principal admitted by typed authorization.
// Password is the secret credential material for NATS user/pass auth; subject
// strings are NATS subtree permissions such as "poppi.mesh.host.op.alice.>".
type AuthorizedPrincipal struct {
	PrincipalID    string
	Password       string
	PublishAllow   []string
	SubscribeAllow []string
}

func prepareAuthorization(in Authorization) (Authorization, error) {
	if !in.Enabled {
		return Authorization{}, nil
	}
	out := in
	if strings.TrimSpace(out.InternalUsername) == "" || strings.TrimSpace(out.InternalPassword) == "" {
		user, pass, err := generateInternalAuth()
		if err != nil {
			return Authorization{}, err
		}
		out.InternalUsername = user
		out.InternalPassword = pass
	}
	seen := map[string]bool{}
	for i := range out.Principals {
		p := &out.Principals[i]
		p.PrincipalID = strings.TrimSpace(p.PrincipalID)
		p.Password = strings.TrimSpace(p.Password)
		if p.PrincipalID == "" {
			return Authorization{}, fmt.Errorf("typed NATS authorization principal %d has no principal id", i)
		}
		if p.PrincipalID == out.InternalUsername {
			return Authorization{}, fmt.Errorf("typed NATS authorization principal %q conflicts with the internal user", p.PrincipalID)
		}
		if seen[p.PrincipalID] {
			return Authorization{}, fmt.Errorf("typed NATS authorization declares principal %q more than once", p.PrincipalID)
		}
		seen[p.PrincipalID] = true
		if p.Password == "" {
			return Authorization{}, fmt.Errorf("typed NATS authorization principal %q has no credential material", p.PrincipalID)
		}
		var err error
		p.PublishAllow, err = validateSubjectSubtrees("publish", p.PrincipalID, p.PublishAllow)
		if err != nil {
			return Authorization{}, err
		}
		p.SubscribeAllow, err = validateSubjectSubtrees("subscribe", p.PrincipalID, p.SubscribeAllow)
		if err != nil {
			return Authorization{}, err
		}
	}
	return out, nil
}

func generateInternalAuth() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("typed NATS authorization could not generate internal credential: %w", err)
	}
	pass := strings.ToLower(strings.TrimRight(base32.StdEncoding.EncodeToString(raw), "="))
	return "_jbmesh_internal", pass, nil
}

func validateSubjectSubtrees(kind, principal string, in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	seen := map[string]bool{}
	for _, subj := range in {
		subj = strings.TrimSpace(subj)
		if err := validateSubjectSubtree(subj); err != nil {
			return nil, fmt.Errorf("typed NATS authorization principal %q has invalid %s subject %q: %w", principal, kind, subj, err)
		}
		if !seen[subj] {
			out = append(out, subj)
			seen[subj] = true
		}
	}
	return out, nil
}

func validateSubjectSubtree(subj string) error {
	if subj == "" {
		return fmt.Errorf("blank subject")
	}
	if subj == ">" {
		return fmt.Errorf("external principals cannot be granted the all-subject wildcard")
	}
	if strings.ContainsAny(subj, " \t\r\n") {
		return fmt.Errorf("subjects cannot contain whitespace")
	}
	parts := strings.Split(subj, ".")
	for i, part := range parts {
		switch {
		case part == "":
			return fmt.Errorf("subjects cannot contain empty tokens")
		case part == "*":
			return fmt.Errorf("single-token wildcard is not a subtree grant")
		case part == ">":
			if i != len(parts)-1 {
				return fmt.Errorf("full wildcard must be the final token")
			}
		case strings.Contains(part, ">") || strings.Contains(part, "*"):
			return fmt.Errorf("wildcards must be whole tokens")
		}
	}
	return nil
}

// LoggingConfig controls human-readable node and embedded-NATS lifecycle logs.
type LoggingConfig struct {
	// Quiet suppresses default package-log output. If Logger or NATSLogger is
	// set, logs are routed there even when Quiet is true.
	Quiet bool

	// Logger receives jb-mesh node/mesh lifecycle logs. Nil preserves the
	// historical behavior of writing through the package log unless Quiet is true.
	Logger *slog.Logger

	// NATSLogger receives embedded NATS server logs. If nil and Logger is set,
	// embedded NATS logs are adapted into Logger. If both are nil and Quiet is
	// true, NATS Options.NoLog is enabled.
	NATSLogger natsserver.Logger

	// NATSDebug and NATSTrace are passed to embedded NATS when a custom logger
	// is configured.
	NATSDebug bool
	NATSTrace bool
}

type nodeLog struct {
	quiet  bool
	logger *slog.Logger
}

func newNodeLog(cfg LoggingConfig) nodeLog {
	return nodeLog{quiet: cfg.Quiet, logger: cfg.Logger}
}

func (l nodeLog) printf(format string, args ...any) {
	if l.logger != nil {
		l.logger.Info(fmt.Sprintf(format, args...))
		return
	}
	if l.quiet {
		return
	}
	log.Printf(format, args...)
}

func (n *Node) logf(format string, args ...any) {
	if n == nil {
		log.Printf(format, args...)
		return
	}
	n.log.printf(format, args...)
}

type slogNATSLogger struct {
	logger *slog.Logger
}

func (l slogNATSLogger) Noticef(format string, args ...any) {
	l.log(slog.LevelInfo, format, args...)
}

func (l slogNATSLogger) Warnf(format string, args ...any) {
	l.log(slog.LevelWarn, format, args...)
}

func (l slogNATSLogger) Fatalf(format string, args ...any) {
	l.log(slog.LevelError, format, args...)
}

func (l slogNATSLogger) Errorf(format string, args ...any) {
	l.log(slog.LevelError, format, args...)
}

func (l slogNATSLogger) Debugf(format string, args ...any) {
	l.log(slog.LevelDebug, format, args...)
}

func (l slogNATSLogger) Tracef(format string, args ...any) {
	l.log(slog.LevelDebug, format, args...)
}

func (l slogNATSLogger) log(level slog.Level, format string, args ...any) {
	if l.logger == nil {
		return
	}
	l.logger.Log(context.Background(), level, fmt.Sprintf(format, args...))
}

// New creates and starts a new node
func New(cfg Config) (*Node, error) {
	if cfg.Authorization.Enabled && cfg.Token != "" {
		return nil, fmt.Errorf("typed NATS authorization cannot be combined with global token auth")
	}
	if cfg.Authorization.Enabled && strings.TrimSpace(cfg.NATSUrl) != "" {
		return nil, fmt.Errorf("typed NATS authorization applies only to embedded NATS seeds; external NATS URLs must provide their own prepared-zone ACL")
	}
	authz := cfg.Authorization
	if authz.Enabled {
		var err error
		authz, err = prepareAuthorization(authz)
		if err != nil {
			return nil, err
		}
	}

	// Load jb-mesh config
	jbCfg, err := config.LoadWithHome(cfg.HomeDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	if err := jbCfg.EnsureDirs(); err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}

	n := &Node{
		cfg:        jbCfg,
		log:        newNodeLog(cfg.Logging),
		logging:    cfg.Logging,
		streamSubs: make(map[string][]*nats.Subscription),
	}

	natsPort := jbCfg.NATS.EmbedPort
	if natsPort == 0 {
		natsPort = DefaultNATSPort
	}
	// cfg.EmbedPort wins over the YAML default. -1 means "pick any free
	// port"; we resolve that on demand by binding once and reading back.
	if cfg.EmbedPort != 0 {
		natsPort = cfg.EmbedPort
	}
	natsHost := strings.TrimSpace(jbCfg.NATS.EmbedHost)
	if strings.TrimSpace(cfg.EmbedHost) != "" {
		natsHost = strings.TrimSpace(cfg.EmbedHost)
	}
	leafPort := cfg.LeafPort
	if leafPort == 0 {
		leafPort = DefaultLeafPort
	}
	leafHost := strings.TrimSpace(jbCfg.NATS.LeafHost)
	if strings.TrimSpace(cfg.LeafHost) != "" {
		leafHost = strings.TrimSpace(cfg.LeafHost)
	}
	startedAt := time.Now().Unix()

	// Determine how to connect to NATS:
	// 1. Explicit --nats URL → pure client mode (no embed, no mDNS)
	// 2. Explicit --seed/--leaf → embed NATS in that role
	// 3. Default → mDNS browse to auto-detect role
	if cfg.NATSUrl == "" {
		role := cfg.Role
		var leafRemotes []string

		// If role forced via --leaf with explicit URL
		if role == "leaf" && cfg.LeafURL != "" {
			leafRemotes = []string{cfg.LeafURL}
		}

		// Auto-detect via mDNS if role not forced
		useMDNS := !cfg.NoMDNS && role == ""
		if useMDNS {
			disc, err := discovery.NewDiscovery(discovery.DiscoveryConfig{
				NodeName: cfg.NodeName,
				NATSPort: natsPort,
				Enabled:  true,
			})
			if err != nil {
				n.logf("[node] warning: failed to create discovery: %v", err)
			} else {
				n.logf("[node] browsing for mesh peers (timeout: %v)...", discovery.BrowseTimeout)
				peers, err := disc.Browse(discovery.BrowseTimeout)
				if err != nil {
					n.logf("[node] warning: mDNS browse failed: %v", err)
				}
				n.logf("[node] mDNS browse found %d peer(s)", len(peers))

				seed := discovery.FindSeed(peers)
				if seed != nil {
					role = "leaf"
					leafRemotes = []string{seed.LeafURL()}
					n.logf("[node] discovered seed %s at %s, joining as leaf", seed.Name, seed.LeafURL())
				} else {
					role = "seed"
					n.logf("[node] no seed found, starting as seed")
				}
			}
		}

		// Default to seed if still undecided (mDNS disabled, no explicit role)
		if role == "" {
			role = "seed"
			n.logf("[node] defaulting to seed role (mDNS disabled)")
		}

		// Start embedded NATS with determined role
		embeddedCfg := embeddedNATSConfig{
			Token:         cfg.Token,
			Authorization: authz,
			StoreDir:      jbCfg.JetStreamDir(),
			Host:          natsHost,
			Port:          natsPort,
			NodeName:      cfg.NodeName,
			Role:          role,
			LeafHost:      leafHost,
			LeafPort:      leafPort,
			LeafRemotes:   leafRemotes,
			WebsocketHost: cfg.WebsocketHost,
			WebsocketPort: cfg.WebsocketPort,
			Logging:       cfg.Logging,
		}
		ns, err := startEmbeddedNATS(embeddedCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to start embedded NATS: %w", err)
		}
		n.natsServer = ns
		n.role = role
		n.startedAt = startedAt
		n.token = cfg.Token
		// If we asked for a random port (-1), resolve to whatever was bound.
		if addr, ok := ns.Addr().(*net.TCPAddr); ok && addr != nil {
			n.natsPort = addr.Port
		} else {
			n.natsPort = natsPort
		}
		n.natsHost = natsHost
		n.leafPort = leafPort
		n.leafHost = leafHost
		cfg.NATSUrl = ns.ClientURL()
		n.logf("[node] embedded NATS server running at %s (role=%s)", cfg.NATSUrl, role)

		// Start mDNS advertisement after NATS is up
		if !cfg.NoMDNS {
			disc, err := discovery.NewDiscovery(discovery.DiscoveryConfig{
				NodeName:  cfg.NodeName,
				NATSPort:  natsPort,
				LeafPort:  leafPort,
				Role:      role,
				Enabled:   true,
				StartedAt: startedAt,
			})
			if err == nil {
				// Wire up split-brain detection: if we're a seed and discover
				// another seed, the newer one demotes to leaf.
				if role == "seed" {
					disc.OnPeerFound(func(peer discovery.Peer) {
						n.onPeerFound(peer)
					})
				}
				if err := disc.Start(); err != nil {
					n.logf("[node] warning: failed to start mDNS advertisement: %v", err)
				} else {
					n.discovery = disc
					n.logf("[node] mDNS discovery active (role=%s)", role)
				}
			}
		}
	}

	// Create tool manager and load installed tools
	manager := tools.NewManager(jbCfg)
	if err := manager.LoadAll(); err != nil {
		n.Close()
		return nil, fmt.Errorf("failed to load tools: %w", err)
	}

	executor := tools.NewExecutor(manager)
	n.manager = manager
	n.executor = executor

	// Connect to NATS mesh
	m, err := mesh.New(mesh.Config{
		NATSUrl:   cfg.NATSUrl,
		NodeName:  cfg.NodeName,
		Token:     cfg.Token,
		Username:  authz.InternalUsername,
		Password:  authz.InternalPassword,
		WebSocket: cfg.NATSWebSocket,
		Logging: mesh.LoggingConfig{
			Quiet:  cfg.Logging.Quiet,
			Logger: cfg.Logging.Logger,
		},
	})
	if err != nil {
		n.Close()
		return nil, fmt.Errorf("failed to join mesh: %w", err)
	}
	n.mesh = m
	n.nodeName = cfg.NodeName
	n.logProducer = logstore.NewProducer(m.Conn(), cfg.NodeName)

	// Initialize event bus for lifecycle event emission
	eventBus := events.NewBusWithLogging(m.Conn(), cfg.NodeName, events.LoggingConfig{
		Quiet:  cfg.Logging.Quiet,
		Logger: cfg.Logging.Logger,
	})
	n.eventBus = eventBus

	if err := n.startLoggingService(); err != nil {
		n.logf("[node] warning: failed to start logging service: %v", err)
	}
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "info", Kind: "node", Message: "node connected", Data: map[string]any{"nats_url": cfg.NATSUrl, "role": n.role}})

	// Inject NATS URL into executor so tools get JB_NATS_URL env var
	executor.SetNATSURL(cfg.NATSUrl)

	// Subscribe management handlers before tool startup so node-directed control
	// requests still work even if a persistent tool blocks registration.
	// Listen for remote install requests
	if err := m.SubscribeInstall(func(source string) (string, string, error) {
		return n.InstallTool(source)
	}); err != nil {
		n.logf("[node] warning: failed to subscribe to install requests: %v", err)
	}

	// Listen for remote uninstall requests
	if err := m.SubscribeUninstall(func(toolName string, removeEnv bool) error {
		return n.UninstallTool(toolName, removeEnv)
	}); err != nil {
		n.logf("[node] warning: failed to subscribe to uninstall requests: %v", err)
	}

	// Listen for remote update requests
	if err := m.SubscribeUpdate(func(toolName string, clean bool) (string, string, error) {
		return n.UpdateTool(toolName, clean)
	}); err != nil {
		n.logf("[node] warning: failed to subscribe to update requests: %v", err)
	}

	// Listen for remote config requests
	if err := m.SubscribeConfig(func(toolName, action string, values map[string]interface{}) (map[string]interface{}, error) {
		return n.HandleConfig(toolName, action, values)
	}); err != nil {
		n.logf("[node] warning: failed to subscribe to config requests: %v", err)
	}

	// Listen for remote release inspection requests
	if err := m.SubscribeReleaseInspect(func(toolName string) (*mesh.ReleaseInspectInfo, error) {
		inspection, err := n.InspectRelease(toolName)
		if err != nil {
			return nil, err
		}
		return releaseInspectionToMesh(inspection), nil
	}); err != nil {
		n.logf("[node] warning: failed to subscribe to release inspection requests: %v", err)
	}

	// Listen for schema requests (node.<nodename>.tools.<tool>.schema)
	if err := m.SubscribeToolSchema(func(toolName string) (map[string]interface{}, error) {
		return n.GetToolSchema(toolName)
	}); err != nil {
		n.logf("[node] warning: failed to subscribe to schema requests: %v", err)
	}

	// Register all installed tools into the mesh.
	// Keep startup deterministic and defer GPU-hinted tools until last so one
	// heavy or flaky local service doesn't block the rest of the node.
	startupTools := manager.List()
	sort.Slice(startupTools, func(i, j int) bool {
		rank := func(t *tools.Tool) int {
			if t.Manifest.Runtime.Mode != "persistent" {
				return 0
			}
			if t.Manifest.Resources.GPU {
				return 2
			}
			return 1
		}
		ri, rj := rank(startupTools[i]), rank(startupTools[j])
		if ri != rj {
			return ri < rj
		}
		return startupTools[i].Name < startupTools[j].Name
	})
	for _, tool := range startupTools {
		if err := n.registerTool(tool); err != nil {
			n.logf("[node] warning: failed to register %s: %v", tool.Name, err)
		}
	}

	// Wire crash monitor event callbacks
	if cm := executor.CrashMonitor(); cm != nil {
		nodeName := cfg.NodeName
		cm.OnCrash(func(toolName string, err error, attempt int) {
			if eventBus != nil {
				_ = eventBus.Emit(events.ToolCrashed(nodeName, toolName, err, attempt))
			}
		})
		cm.OnGiveUp(func(toolName string, failures int) {
			if eventBus != nil {
				_ = eventBus.Emit(events.ToolCrashed(nodeName, toolName,
					fmt.Errorf("gave up after %d consecutive failures", failures), failures))
			}
		})
	}

	// Emit node.joined event
	if err := eventBus.Emit(events.NodeJoined(cfg.NodeName, map[string]interface{}{
		"tools": len(manager.List()),
	})); err != nil {
		n.logf("[node] warning: failed to emit node.joined: %v", err)
	}

	// Register file store NATS handlers (shared bucket across all nodes)
	js, err := m.Conn().JetStream()
	if err != nil {
		n.logf("[node] warning: failed to get JetStream context: %v", err)
	} else {
		store, err := filestore.NewStore(js, filestore.DefaultConfig())
		if err != nil {
			n.logf("[node] warning: failed to create file store: %v", err)
		} else {
			if err := m.SubscribeFileHandlers(store); err != nil {
				n.logf("[node] warning: failed to register file store handlers: %v", err)
			}
			m.SetFileStore(store) // Enable file param resolution in tool calls
		}
	}

	return n, nil
}

// embeddedNATSConfig holds configuration for starting an embedded NATS server.
const (
	DefaultNATSPort = 4222
	DefaultLeafPort = 7422
)

type embeddedNATSConfig struct {
	Token         string
	Authorization Authorization
	StoreDir      string
	Host          string // client bind host; empty preserves historical 0.0.0.0
	Port          int    // client port (default 4222)
	NodeName      string
	Role          string // "seed" or "leaf"
	WebsocketHost string // optional websocket bind host
	WebsocketPort int    // optional websocket bind port
	Logging       LoggingConfig

	// Seed-only: port for incoming leaf node connections (default 7422)
	LeafHost string // empty preserves historical 0.0.0.0
	LeafPort int

	// Leaf-only: seed URLs to connect to (nats-leaf://host:port)
	LeafRemotes []string
}

// startEmbeddedNATS starts a NATS server in-process with JetStream enabled.
//
// In seed mode, the server listens for client connections and incoming leaf
// node connections. JetStream runs in single-server mode (no Raft).
//
// In leaf mode, the server listens for local client connections and connects
// outbound to the seed's leaf port. NATS transparently bridges all traffic
// between local clients and the seed. JetStream API requests from leaf clients
// are forwarded to the seed.
func startEmbeddedNATS(cfg embeddedNATSConfig) (*natsserver.Server, error) {
	port := cfg.Port
	if port == 0 {
		port = DefaultNATSPort
	}
	host := cfg.Host
	if host == "" {
		host = "0.0.0.0"
	}
	logCfg := newNodeLog(cfg.Logging)

	opts := &natsserver.Options{
		Host:           host,
		Port:           port,
		ServerName:     cfg.NodeName,
		NoLog:          cfg.Logging.Quiet && cfg.Logging.Logger == nil && cfg.Logging.NATSLogger == nil,
		NoSigs:         true,
		MaxControlLine: 4096,
		JetStream:      true,
		StoreDir:       cfg.StoreDir,
	}

	if cfg.Token != "" {
		opts.Authorization = cfg.Token
	}
	if cfg.Authorization.Enabled {
		users := []*natsserver.User{{
			Username: cfg.Authorization.InternalUsername,
			Password: cfg.Authorization.InternalPassword,
			Permissions: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: []string{">"}},
				Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
			},
		}}
		for _, p := range cfg.Authorization.Principals {
			users = append(users, &natsserver.User{
				Username: p.PrincipalID,
				Password: p.Password,
				Permissions: &natsserver.Permissions{
					Publish:   &natsserver.SubjectPermission{Allow: append([]string(nil), p.PublishAllow...)},
					Subscribe: &natsserver.SubjectPermission{Allow: append([]string(nil), p.SubscribeAllow...)},
				},
			})
		}
		opts.Users = users
	}

	if cfg.WebsocketPort != 0 {
		wsHost := cfg.WebsocketHost
		if wsHost == "" {
			wsHost = "127.0.0.1"
		}
		opts.Websocket = natsserver.WebsocketOpts{
			Host:  wsHost,
			Port:  cfg.WebsocketPort,
			NoTLS: true, // Portal terminates public TLS; keep lab listener private.
		}
		logCfg.printf("[node] NATS websocket listener enabled on %s:%d", wsHost, cfg.WebsocketPort)
	}

	switch cfg.Role {
	case "seed":
		leafPort := cfg.LeafPort
		if leafPort == 0 {
			leafPort = DefaultLeafPort
		}
		leafHost := cfg.LeafHost
		if leafHost == "" {
			leafHost = "0.0.0.0"
		}
		opts.LeafNode = natsserver.LeafNodeOpts{
			Host: leafHost,
			Port: leafPort,
		}
		logCfg.printf("[node] NATS seed: clients on %s:%d, leaf connections on %s:%d", host, port, leafHost, leafPort)

	case "leaf":
		var remotes []*natsserver.RemoteLeafOpts
		for _, remote := range cfg.LeafRemotes {
			u, err := url.Parse(remote)
			if err != nil {
				logCfg.printf("[node] warning: invalid leaf remote URL %q: %v", remote, err)
				continue
			}
			remotes = append(remotes, &natsserver.RemoteLeafOpts{
				URLs: []*url.URL{u},
			})
		}
		if len(remotes) == 0 {
			return nil, fmt.Errorf("leaf mode requires at least one remote URL")
		}
		opts.LeafNode = natsserver.LeafNodeOpts{
			Remotes: remotes,
		}
		logCfg.printf("[node] NATS leaf: clients on %s:%d, connecting to seed(s): %v", host, port, cfg.LeafRemotes)

	default:
		return nil, fmt.Errorf("unknown NATS role: %q (expected \"seed\" or \"leaf\")", cfg.Role)
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, err
	}

	if cfg.Logging.NATSLogger != nil {
		ns.SetLogger(cfg.Logging.NATSLogger, cfg.Logging.NATSDebug, cfg.Logging.NATSTrace)
	} else if cfg.Logging.Logger != nil {
		ns.SetLogger(slogNATSLogger{logger: cfg.Logging.Logger}, cfg.Logging.NATSDebug, cfg.Logging.NATSTrace)
	} else {
		ns.ConfigureLogger()
	}
	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		return nil, fmt.Errorf("NATS server failed to start within 5s")
	}

	return ns, nil
}

// registerTool registers a single tool's methods into the NATS mesh
func (n *Node) registerTool(tool *tools.Tool) error {
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{
		Level:   "info",
		Kind:    "tool",
		Tool:    tool.Name,
		Message: "tool registration started",
		Data:    map[string]any{"version": tool.Manifest.Version, "mode": tool.Manifest.Runtime.Mode},
	})
	manifest := tool.Manifest

	// Collect method names
	var methods []string
	for name := range manifest.RPC.Methods {
		methods = append(methods, name)
	}

	if len(methods) == 0 {
		return fmt.Errorf("tool %s has no methods", tool.Name)
	}

	// Auto-start persistent tools
	if manifest.Runtime.Mode == "persistent" {
		n.logf("[node] starting persistent tool %s...", tool.Name)
		startAt := time.Now()
		if err := n.executor.Start(tool.Name); err != nil {
			logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{
				Level:    "error",
				Kind:     "tool",
				Tool:     tool.Name,
				Duration: time.Since(startAt),
				OK:       logstore.BoolPtr(false),
				Message:  "tool start failed",
				Data:     map[string]any{"error": err.Error()},
			})
			return fmt.Errorf("failed to start persistent tool %s: %w", tool.Name, err)
		}
		logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{
			Level:    "info",
			Kind:     "tool",
			Tool:     tool.Name,
			Duration: time.Since(startAt),
			OK:       logstore.BoolPtr(true),
			Message:  "tool started",
			Data:     map[string]any{"mode": manifest.Runtime.Mode},
		})
		// Emit tool.started event
		if n.eventBus != nil {
			if emitErr := n.eventBus.Emit(events.ToolStarted(n.nodeName, tool.Name)); emitErr != nil {
				n.logf("[node] warning: failed to emit tool.started: %v", emitErr)
			}
		}
	}

	// Build method schemas from manifest input schemas. Streaming-method
	// markers come from manifest.RPC.Methods[m].Stream and ride along in
	// the same metadata so discovery-side consumers can distinguish
	// streaming from non-streaming without extra round trips.
	methodSchemas := make(map[string]mesh.MethodSchema)
	for methodName, method := range manifest.RPC.Methods {
		var ms mesh.MethodSchema
		if method.Input != nil {
			ms = manifestSchemaToMeshSchema(method.Input)
		}
		ms.Stream = method.Stream
		methodSchemas[methodName] = ms
	}

	// Register streaming subjects (Phase 2) for any methods declared with
	// `stream: true` in the manifest. These are registered BEFORE the
	// single-reply micro endpoints so a streaming method has both subjects
	// live; old callers using Mesh.Call continue to work, new callers use
	// Mesh.Stream against the .stream subject.
	if err := n.registerStreamingSubjects(tool, manifest); err != nil {
		// Log but don't fail registration; the single-reply path still works.
		n.logf("[node] warning: streaming subjects for %s: %v", tool.Name, err)
	}

	// Register into mesh — the handler bridges NATS calls to jumpboot.
	//
	// When the incoming CallRequest carries a CallID (set by mesh.CallWithContext
	// on the caller side), we subscribe to `cancel.<CallID>` for the call's
	// duration and route through executor.CallContext so a cancel publish
	// reaches the jumpboot tool process. Calls without a CallID use the
	// original (non-cancellable) executor.Call path.
	return n.mesh.RegisterTool(
		tool.Name,
		manifest.Version,
		manifest.Description,
		methods,
		func(req mesh.CallRequest, method string, params map[string]interface{}) (interface{}, error) {
			started := time.Now()
			corr := strings.TrimSpace(req.Corr)
			if corr == "" {
				if v, ok := params["corr"].(string); ok {
					corr = strings.TrimSpace(v)
				}
			}

			var (
				result   interface{}
				err      error
				cancelID = strings.TrimSpace(req.CallID)
			)

			if cancelID == "" {
				// No CallID — non-cancellable path (legacy callers).
				result, err = n.executor.Call(tool.Name, method, params)
			} else {
				// Cancel-aware path: subscribe to cancel.<CallID> for the call's
				// duration. When a cancel arrives, we cancel a context that the
				// executor threads down to jumpboot's QueueProcess.CallContext.
				ctx, cancel := context.WithCancel(context.Background())
				sub, subErr := n.mesh.Conn().Subscribe(mesh.CancelSubject(cancelID), func(_ *nats.Msg) {
					cancel()
				})
				if subErr != nil {
					n.logf("[node] cancel-subject subscribe failed for %s: %v", cancelID, subErr)
					// Continue without cancellation rather than failing the call.
					result, err = n.executor.Call(tool.Name, method, params)
				} else {
					result, err = n.executor.CallContext(ctx, tool.Name, method, params)
					_ = sub.Unsubscribe()
				}
				cancel()
			}

			data := map[string]any{}
			if err != nil {
				data["error"] = err.Error()
			}
			if result != nil {
				if b, mErr := json.Marshal(result); mErr == nil {
					data["response_bytes"] = len(b)
				}
			}
			logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{
				Level:    levelForCall(err),
				Kind:     "tool_call",
				Tool:     tool.Name,
				Method:   method,
				Corr:     corr,
				Duration: time.Since(started),
				OK:       logstore.BoolPtr(err == nil),
				Message:  messageForCall(tool.Name, method, err),
				Data:     data,
			})
			return result, err
		},
		methodSchemas,
	)
}

// registerStreamingSubjects wires raw NATS subscriptions for streaming
// methods. Each streaming method
// gets two subjects: the load-balanced `tools.<tool>.<method>.stream` and
// the node-targeted `node.<node>.tools.<tool>.<method>.stream`.
//
// We use raw nc.Subscribe rather than NATS micro because micro's request/
// reply model only allows one Respond per request; streaming needs to
// publish multiple frames to the caller's reply inbox.
func (n *Node) registerStreamingSubjects(tool *tools.Tool, manifest *config.Manifest) error {
	// First, tear down any existing streaming subs for this tool. This is
	// load-bearing for `update --clean` and any re-registration path —
	// without it we'd accumulate duplicate subscribers on the same subject
	// and each frame would be delivered N times.
	n.unregisterStreamingSubjects(tool.Name)

	var subs []*nats.Subscription
	for methodName, methodDef := range manifest.RPC.Methods {
		if !methodDef.Stream {
			continue
		}
		// Capture for closure.
		mName := methodName
		toolName := tool.Name

		subject := mesh.StreamSubject(toolName, mName)
		nodeSubject := mesh.NodeStreamSubject(n.nodeName, toolName, mName)

		handler := func(msg *nats.Msg) {
			n.handleStreamRequest(toolName, mName, msg)
		}

		s1, err := n.mesh.Conn().Subscribe(subject, handler)
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", subject, err)
		}
		s2, err := n.mesh.Conn().Subscribe(nodeSubject, handler)
		if err != nil {
			_ = s1.Unsubscribe()
			return fmt.Errorf("subscribe %s: %w", nodeSubject, err)
		}
		subs = append(subs, s1, s2)
		n.logf("[mesh] registered streaming %s", subject)
	}

	if len(subs) > 0 {
		n.streamSubsMu.Lock()
		n.streamSubs[tool.Name] = subs
		n.streamSubsMu.Unlock()
	}
	return nil
}

// unregisterStreamingSubjects removes the raw NATS subscriptions added by
// registerStreamingSubjects for the given tool. Idempotent — safe to call
// for tools that never registered streaming subjects.
func (n *Node) unregisterStreamingSubjects(toolName string) {
	n.streamSubsMu.Lock()
	subs := n.streamSubs[toolName]
	delete(n.streamSubs, toolName)
	n.streamSubsMu.Unlock()
	for _, s := range subs {
		if err := s.Unsubscribe(); err != nil {
			n.logf("[node] unsubscribe streaming for %s: %v", toolName, err)
		}
	}
}

// handleStreamRequest is the raw NATS handler invoked for streaming tool
// calls. It pumps frames from executor.CallStream to the caller's reply
// inbox, marking the terminal frame with done=true. The call's context is
// cancellable via the `cancel.<call_id>` subject convention shared with
// single-reply calls.
func (n *Node) handleStreamRequest(toolName, methodName string, msg *nats.Msg) {
	started := time.Now()

	var req mesh.CallRequest
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			n.publishStreamErrorFrame(msg.Reply, fmt.Sprintf("invalid request: %v", err))
			return
		}
	}
	if req.Params == nil {
		req.Params = make(map[string]interface{})
	}

	callID := strings.TrimSpace(req.CallID)
	corr := strings.TrimSpace(req.Corr)
	if corr == "" {
		if v, ok := req.Params["corr"].(string); ok {
			corr = strings.TrimSpace(v)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Per-call cancel subscription (same pattern as single-reply path).
	if callID != "" {
		cancelSub, err := n.mesh.Conn().Subscribe(mesh.CancelSubject(callID), func(_ *nats.Msg) {
			cancel()
		})
		if err != nil {
			n.logf("[node] stream cancel-sub for %s failed: %v", callID, err)
		} else {
			defer cancelSub.Unsubscribe()
		}
	}

	frames, err := n.executor.CallStream(ctx, toolName, methodName, req.Params)
	if err != nil {
		logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{
			Level: "error", Kind: "tool_call", Tool: toolName, Method: methodName, Corr: corr,
			Duration: time.Since(started), OK: logstore.BoolPtr(false),
			Message: fmt.Sprintf("%s.%s stream failed at setup", toolName, methodName),
			Data:    map[string]any{"error": err.Error()},
		})
		n.publishStreamErrorFrame(msg.Reply, err.Error())
		return
	}

	framesEmitted := 0
	var streamErr error
	for frame := range frames {
		// Strip request_id (internal to jumpboot) and stamp the serving node.
		delete(frame, "request_id")
		if _, ok := frame["node"]; !ok {
			frame["node"] = n.nodeName
		}
		data, mErr := json.Marshal(frame)
		if mErr != nil {
			n.logf("[node] stream frame marshal for %s.%s: %v", toolName, methodName, mErr)
			continue
		}
		if pubErr := n.mesh.Conn().Publish(msg.Reply, data); pubErr != nil {
			n.logf("[node] stream frame publish for %s.%s: %v", toolName, methodName, pubErr)
			streamErr = pubErr
			break
		}
		framesEmitted++
	}

	level := "info"
	okFlag := true
	if streamErr != nil || ctx.Err() != nil {
		level = "error"
		okFlag = false
	}
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{
		Level: level, Kind: "tool_call", Tool: toolName, Method: methodName, Corr: corr,
		Duration: time.Since(started), OK: logstore.BoolPtr(okFlag),
		Message: fmt.Sprintf("%s.%s stream completed (%d frames)", toolName, methodName, framesEmitted),
		Data:    map[string]any{"frames": framesEmitted, "ctx_err": ctxErrString(ctx)},
	})
}

func ctxErrString(ctx context.Context) string {
	if err := ctx.Err(); err != nil {
		return err.Error()
	}
	return ""
}

func (n *Node) publishStreamErrorFrame(replyInbox, errMsg string) {
	if replyInbox == "" {
		return
	}
	frame := mesh.StreamFrame{Error: errMsg, Done: true, Node: n.nodeName}
	data, err := json.Marshal(frame)
	if err != nil {
		n.logf("[node] stream error-frame marshal: %v", err)
		return
	}
	if pubErr := n.mesh.Conn().Publish(replyInbox, data); pubErr != nil {
		n.logf("[node] stream error-frame publish: %v", pubErr)
	}
}

// InstallTool installs a tool from source (git URL or local path),
// creates its jumpboot environment, and registers it into the mesh — all live.
func (n *Node) InstallTool(source string) (string, string, error) {
	started := time.Now()
	tool, err := n.manager.Install(source)
	if err != nil {
		return "", "", err
	}

	if err := n.registerTool(tool); err != nil {
		logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "error", Kind: "tool", Tool: tool.Name, Duration: time.Since(started), OK: logstore.BoolPtr(false), Message: "tool install failed", Data: map[string]any{"source": source, "error": err.Error()}})
		return tool.Name, tool.Manifest.Version, fmt.Errorf("installed but failed to register: %w", err)
	}

	n.logf("[node] installed and registered %s v%s from %s", tool.Name, tool.Manifest.Version, source)
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "info", Kind: "tool", Tool: tool.Name, Duration: time.Since(started), OK: logstore.BoolPtr(true), Message: "tool installed", Data: map[string]any{"version": tool.Manifest.Version, "source": source}})

	// Emit tool.installed event
	if n.eventBus != nil {
		if err := n.eventBus.Emit(events.ToolInstalled(n.nodeName, tool.Name, tool.Manifest.Version)); err != nil {
			n.logf("[node] warning: failed to emit tool.installed: %v", err)
		}
	}

	return tool.Name, tool.Manifest.Version, nil
}

func releaseInspectionToMesh(in *tools.ReleaseInspection) *mesh.ReleaseInspectInfo {
	if in == nil {
		return nil
	}
	var repo *mesh.ReleaseInspectRepoState
	if in.Repo != nil {
		repo = &mesh.ReleaseInspectRepoState{
			Root:           in.Repo.Root,
			ResolvedPath:   in.Repo.ResolvedPath,
			Branch:         in.Repo.Branch,
			Commit:         in.Repo.Commit,
			Upstream:       in.Repo.Upstream,
			UpstreamCommit: in.Repo.UpstreamCommit,
			Ahead:          in.Repo.Ahead,
			Behind:         in.Repo.Behind,
			StatusShort:    in.Repo.StatusShort,
			Dirty:          in.Repo.Dirty,
		}
	}
	return &mesh.ReleaseInspectInfo{
		ToolName:        in.ToolName,
		ToolPath:        in.ToolPath,
		ManifestVersion: in.ManifestVersion,
		Repo:            repo,
	}
}

// InspectRelease returns deploy/release inspection info for a tool on this node.
func (n *Node) InspectRelease(toolName string) (*tools.ReleaseInspection, error) {
	return n.manager.InspectRelease(toolName)
}

// HandleConfig handles get/set config requests for a tool.
func (n *Node) HandleConfig(toolName, action string, values map[string]interface{}) (map[string]interface{}, error) {
	tool, ok := n.manager.Get(toolName)
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", toolName)
	}

	store := tools.NewToolConfigStore()

	switch action {
	case "get":
		return store.Resolve(tool, nil)
	case "set":
		if err := store.Save(tool, values); err != nil {
			return nil, err
		}
		// Emit tool.configured event
		if n.eventBus != nil {
			if err := n.eventBus.Emit(events.ToolConfigured(n.nodeName, toolName, values)); err != nil {
				n.logf("[node] warning: failed to emit tool.configured: %v", err)
			}
		}
		// If tool is running and persistent, restart it to pick up new config
		if tool.Status == "running" && tool.Manifest.Runtime.Mode == "persistent" {
			n.logf("[node] config changed for running tool %s — restarting", toolName)
			if err := n.executor.Stop(toolName); err != nil {
				return nil, fmt.Errorf("failed to stop for config reload: %w", err)
			}
			if err := n.executor.Start(toolName); err != nil {
				return nil, fmt.Errorf("failed to restart after config change: %w", err)
			}
		}
		return store.Resolve(tool, nil)
	default:
		return nil, fmt.Errorf("unknown config action: %s", action)
	}
}

// UpdateTool updates a tool's source and re-registers it in the mesh.
// Returns (oldVersion, newVersion, error).
func (n *Node) UpdateTool(toolName string, clean bool) (string, string, error) {
	started := time.Now()
	tool, ok := n.manager.Get(toolName)
	if !ok {
		return "", "", fmt.Errorf("tool not found: %s", toolName)
	}

	oldVersion := tool.Manifest.Version
	wasRunning := tool.Status == "running"
	n.logf("[node] UpdateTool start: tool=%s oldVersion=%s wasRunning=%t clean=%t", toolName, oldVersion, wasRunning, clean)

	// Stop if running — with hard timeout to survive mutex contention.
	if wasRunning {
		n.logf("[node] UpdateTool stopping %s", toolName)
		stopDone := make(chan error, 1)
		go func() {
			stopDone <- n.executor.Stop(toolName)
		}()
		select {
		case err := <-stopDone:
			if err != nil {
				n.logf("[node] UpdateTool stop error for %s: %v", toolName, err)
				// Stop failed — try force-kill via executor if possible
				if fErr := n.executor.ForceKill(toolName); fErr != nil {
					n.logf("[node] UpdateTool force-kill also failed: %v", fErr)
				}
			}
		case <-time.After(15 * time.Second):
			n.logf("[node] UpdateTool stop timed out after 15s — force-killing %s", toolName)
			if fErr := n.executor.ForceKill(toolName); fErr != nil {
				n.logf("[node] UpdateTool force-kill error: %v", fErr)
			}
		}
		if n.eventBus != nil {
			_ = n.eventBus.Emit(events.ToolStopped(n.nodeName, toolName, "update"))
		}
	}

	// Unregister old version from mesh
	n.logf("[node] UpdateTool unregistering %s from mesh", toolName)
	n.mesh.UnregisterTool(toolName)

	// Update the tool
	n.logf("[node] UpdateTool reloading manifest/packages for %s", toolName)
	updatedTool, err := n.manager.Update(toolName, clean)
	if err != nil {
		logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "error", Kind: "tool", Tool: toolName, Duration: time.Since(started), OK: logstore.BoolPtr(false), Message: "tool update failed", Data: map[string]any{"from_version": oldVersion, "clean": clean, "error": err.Error()}})
		return oldVersion, "", err
	}

	// Re-register with new version
	n.logf("[node] UpdateTool re-registering %s version=%s", toolName, updatedTool.Manifest.Version)
	if err := n.registerTool(updatedTool); err != nil {
		logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "error", Kind: "tool", Tool: toolName, Duration: time.Since(started), OK: logstore.BoolPtr(false), Message: "tool update failed", Data: map[string]any{"from_version": oldVersion, "to_version": updatedTool.Manifest.Version, "error": err.Error()}})
		return oldVersion, updatedTool.Manifest.Version, fmt.Errorf("updated but failed to re-register: %w", err)
	}

	n.logf("[node] updated %s: v%s → v%s", toolName, oldVersion, updatedTool.Manifest.Version)
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "info", Kind: "tool", Tool: toolName, Duration: time.Since(started), OK: logstore.BoolPtr(true), Message: "tool updated", Data: map[string]any{"from_version": oldVersion, "to_version": updatedTool.Manifest.Version, "clean": clean}})

	// Emit tool.installed event (with new version — update is effectively reinstall)
	if n.eventBus != nil {
		if err := n.eventBus.Emit(events.ToolInstalled(n.nodeName, toolName, updatedTool.Manifest.Version)); err != nil {
			n.logf("[node] warning: failed to emit tool.installed (update): %v", err)
		}
	}

	return oldVersion, updatedTool.Manifest.Version, nil
}

// UninstallTool stops a tool if running, de-registers from mesh, and removes it.
func (n *Node) UninstallTool(toolName string, removeEnv bool) error {
	started := time.Now()
	// Stop if running
	if err := n.executor.Stop(toolName); err == nil {
		// Tool was running and stopped successfully
		if n.eventBus != nil {
			_ = n.eventBus.Emit(events.ToolStopped(n.nodeName, toolName, "uninstall"))
		}
	}

	// De-register from mesh
	n.mesh.UnregisterTool(toolName)

	// Tear down any streaming subscriptions this tool had (Phase 2).
	// Mesh.UnregisterTool doesn't know about them — they're raw nc.Subscribe
	// subscriptions owned by the Node.
	n.unregisterStreamingSubjects(toolName)

	// Remove from manager (files + optionally env)
	if err := n.manager.Uninstall(toolName, removeEnv); err != nil {
		logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "error", Kind: "tool", Tool: toolName, Duration: time.Since(started), OK: logstore.BoolPtr(false), Message: "tool uninstall failed", Data: map[string]any{"remove_env": removeEnv, "error": err.Error()}})
		return err
	}

	n.logf("[node] uninstalled %s", toolName)
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "info", Kind: "tool", Tool: toolName, Duration: time.Since(started), OK: logstore.BoolPtr(true), Message: "tool uninstalled", Data: map[string]any{"remove_env": removeEnv}})

	// Emit tool.removed event
	if n.eventBus != nil {
		if err := n.eventBus.Emit(events.ToolRemoved(n.nodeName, toolName)); err != nil {
			n.logf("[node] warning: failed to emit tool.removed: %v", err)
		}
	}

	return nil
}

// Call invokes a tool method anywhere in the mesh
func (n *Node) Call(toolName, method string, params map[string]interface{}, targetNode ...string) (*mesh.CallResult, error) {
	return n.mesh.Call(toolName, method, params, 5*time.Minute, targetNode...)
}

// ListMeshServices returns all services visible in the mesh
func (n *Node) ListMeshServices() ([]map[string]interface{}, error) {
	infos, err := n.mesh.ListServices()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for _, info := range infos {
		result = append(result, map[string]interface{}{
			"name":     info.Name,
			"id":       info.ID,
			"version":  info.Version,
			"metadata": info.Metadata,
		})
	}
	return result, nil
}

// ListLocalTools returns tools installed on this node
func (n *Node) ListLocalTools() []*tools.Tool {
	return n.manager.List()
}

// Mesh returns the underlying mesh connection
func (n *Node) Mesh() *mesh.Mesh {
	return n.mesh
}

// Role returns the node's current role ("seed" or "leaf")
func (n *Node) Role() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// GetToolSchema returns the __jb_schema__ for a specific tool via the executor
// This is used by SubscribeToolSchema to handle schema requests from other nodes
// Prefers manifest-based schemas when available (no REPL spinup needed)
func (n *Node) GetToolSchema(toolName string) (map[string]interface{}, error) {
	tool, ok := n.manager.Get(toolName)
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", toolName)
	}

	// Use manifest schemas if available (fast, no REPL spinup)
	if len(tool.Manifest.RPC.Methods) > 0 {
		schema := make(map[string]interface{})
		for methodName, method := range tool.Manifest.RPC.Methods {
			if method.Input != nil {
				methodSchema := schemaToMap(method.Input)
				schema[methodName] = methodSchema
			} else {
				schema[methodName] = map[string]interface{}{
					"type":       "object",
					"properties": map[string]interface{}{},
				}
			}
		}
		return schema, nil
	}

	// No manifest schemas — fall back to REPL (persistent tools)
	return n.executor.GetSchema(toolName)
}

// schemaToMap converts a config.Schema to a map[string]interface{}
// suitable for JSON serialization
func schemaToMap(s *config.Schema) map[string]interface{} {
	if s == nil {
		return nil
	}
	result := make(map[string]interface{})
	if s.Type != "" {
		result["type"] = s.Type
	}
	if s.Desc != "" {
		result["description"] = s.Desc
	}
	if s.Default != nil {
		result["default"] = s.Default
	}
	if len(s.Required) > 0 {
		result["required"] = s.Required
	}
	if len(s.Properties) > 0 {
		props := make(map[string]interface{})
		for name, prop := range s.Properties {
			props[name] = schemaToMap(prop)
		}
		result["properties"] = props
	}
	if s.Items != nil {
		result["items"] = schemaToMap(s.Items)
	}
	return result
}

// manifestSchemaToMeshSchema converts a config.Schema to mesh.MethodSchema for endpoint metadata.
func manifestSchemaToMeshSchema(s *config.Schema) mesh.MethodSchema {
	if s == nil {
		return mesh.MethodSchema{}
	}
	return mesh.MethodSchema{
		Type:        s.Type,
		Description: s.Desc,
		Properties:  schemaToPropertiesMap(s.Properties),
		Required:    s.Required,
	}
}

// schemaToPropertiesMap converts a map of config.Schema to a map[string]interface{}
func schemaToPropertiesMap(props map[string]*config.Schema) map[string]interface{} {
	if len(props) == 0 {
		return nil
	}
	result := make(map[string]interface{})
	for name, prop := range props {
		result[name] = schemaToMap(prop)
	}
	return result
}

// shouldDemote returns true if this node should demote to leaf
// in favor of the other seed. The older seed (lower StartedAt) wins.
// If timestamps match, lower alphabetical node name wins.
func (n *Node) shouldDemote(otherSeed discovery.Peer) bool {
	if n.startedAt > otherSeed.StartedAt {
		return true // we started later → demote
	}
	if n.startedAt == otherSeed.StartedAt {
		return n.nodeName > otherSeed.Name // alphabetical tiebreak
	}
	return false // we started earlier → keep seed
}

// onPeerFound handles mDNS peer discovery. If we're a seed and discover
// another seed, triggers split-brain resolution.
func (n *Node) onPeerFound(peer discovery.Peer) {
	if peer.Role != "seed" {
		return
	}

	n.mu.Lock()
	if n.role != "seed" {
		n.mu.Unlock()
		return // already demoted or not a seed
	}
	n.mu.Unlock()

	if n.shouldDemote(peer) {
		n.logf("[node] split-brain detected: found older seed %s (started=%d, ours=%d). Demoting to leaf.",
			peer.Name, peer.StartedAt, n.startedAt)
		go n.demoteToLeaf(peer) // run async to not block browse loop
	} else {
		n.logf("[node] split-brain detected: we are the older seed (started=%d, %s=%d). Keeping seed role.",
			n.startedAt, peer.Name, peer.StartedAt)
	}
}

// demoteToLeaf transitions this node from seed to leaf, connecting
// to the given seed peer. This involves:
// 1. Closing the mesh client connection
// 2. Shutting down the current NATS server
// 3. Starting a new NATS server in leaf mode
// 4. Reconnecting the mesh client
// 5. Re-registering all tools
// 6. Updating mDNS advertisement
func (n *Node) demoteToLeaf(seed discovery.Peer) {
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "warn", Kind: "node", Message: "node demoting to leaf", Data: map[string]any{"seed": seed.Name, "seed_started_at": seed.StartedAt}})
	n.mu.Lock()
	if n.role != "seed" {
		n.mu.Unlock()
		return // already demoted (race guard)
	}
	n.role = "leaf"
	n.mu.Unlock()

	leafURL := seed.LeafURL()
	seedClientURL := seed.ClientURL()
	n.logf("[node] demoting to leaf: disconnecting from current NATS...")

	// 1. Close mesh client (unsubscribes all handlers)
	if n.mesh != nil {
		n.mesh.Close()
		n.mesh = nil
	}

	// 2. Shutdown current NATS seed server
	if n.natsServer != nil {
		n.natsServer.Shutdown()
		n.natsServer = nil
	}

	// 3. Start new NATS in leaf mode
	// Brief port gap: tool processes using localhost:4222 will get
	// disconnected and auto-reconnect once the new server is up.
	embeddedCfg := embeddedNATSConfig{
		Token:       n.token,
		StoreDir:    n.cfg.JetStreamDir(),
		Host:        n.natsHost,
		Port:        n.natsPort,
		NodeName:    n.nodeName,
		Role:        "leaf",
		LeafRemotes: []string{leafURL},
		Logging:     n.logging,
	}
	ns, err := startEmbeddedNATS(embeddedCfg)
	if err != nil {
		n.logf("[node] FATAL: failed to restart NATS as leaf: %v", err)
		n.logf("[node] node is in a broken state — manual restart required")
		return
	}
	n.natsServer = ns
	n.logf("[node] NATS restarted as leaf, connecting to seed %s", leafURL)

	// 4. Reconnect mesh client to the active seed client URL so leaf-published
	// structured logs/events flow onto the shared mesh instead of staying local-only.
	m, err := mesh.New(mesh.Config{
		NATSUrl:  seedClientURL,
		NodeName: n.nodeName,
		Token:    n.token,
		Logging: mesh.LoggingConfig{
			Quiet:  n.logging.Quiet,
			Logger: n.logging.Logger,
		},
	})
	if err != nil {
		n.logf("[node] FATAL: failed to reconnect mesh: %v", err)
		return
	}
	n.mesh = m

	// Re-init event bus on new connection
	n.eventBus = events.NewBusWithLogging(m.Conn(), n.nodeName, events.LoggingConfig{
		Quiet:  n.logging.Quiet,
		Logger: n.logging.Logger,
	})

	// Inject the active seed client URL into executor so tool-side producers publish onto
	// the shared mesh/logstore rather than the local embedded leaf listener.
	n.executor.SetNATSURL(seedClientURL)

	// 5. Re-register all tools
	for _, tool := range n.manager.List() {
		if err := n.registerTool(tool); err != nil {
			n.logf("[node] warning: failed to re-register %s after demotion: %v", tool.Name, err)
		}
	}

	// Re-subscribe management handlers
	_ = m.SubscribeInstall(func(source string) (string, string, error) {
		return n.InstallTool(source)
	})
	_ = m.SubscribeUninstall(func(toolName string, removeEnv bool) error {
		return n.UninstallTool(toolName, removeEnv)
	})
	_ = m.SubscribeUpdate(func(toolName string, clean bool) (string, string, error) {
		return n.UpdateTool(toolName, clean)
	})
	_ = m.SubscribeConfig(func(toolName, action string, values map[string]interface{}) (map[string]interface{}, error) {
		return n.HandleConfig(toolName, action, values)
	})
	_ = m.SubscribeReleaseInspect(func(toolName string) (*mesh.ReleaseInspectInfo, error) {
		inspection, err := n.InspectRelease(toolName)
		if err != nil {
			return nil, err
		}
		return releaseInspectionToMesh(inspection), nil
	})

	// Re-register file store handlers
	js, err := m.Conn().JetStream()
	if err == nil {
		store, err := filestore.NewStore(js, filestore.DefaultConfig())
		if err == nil {
			_ = m.SubscribeFileHandlers(store)
			m.SetFileStore(store) // Enable file param resolution in tool calls
		}
	}

	// Emit event
	if n.eventBus != nil {
		_ = n.eventBus.Emit(events.NodeJoined(n.nodeName, map[string]interface{}{
			"tools":   len(n.manager.List()),
			"demoted": true,
			"seed":    seed.Name,
		}))
	}

	// 6. Update mDNS advertisement to leaf role
	if n.discovery != nil {
		n.discovery.Stop()
		disc, err := discovery.NewDiscovery(discovery.DiscoveryConfig{
			NodeName:  n.nodeName,
			NATSPort:  n.natsPort,
			Role:      "leaf",
			Enabled:   true,
			StartedAt: n.startedAt,
		})
		if err == nil {
			if err := disc.Start(); err != nil {
				n.logf("[node] warning: failed to restart mDNS as leaf: %v", err)
			} else {
				n.discovery = disc
			}
		}
	}

	n.logf("[node] demotion complete: now running as leaf node connected to seed %s", seed.Name)
	n.logProducer = logstore.NewProducer(n.mesh.Conn(), n.nodeName)
	logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "info", Kind: "node", Message: "node connected", Data: map[string]any{"role": "leaf", "seed": seed.Name}})
}

// Close shuts down the node
func (n *Node) Close() {
	// Emit node.left before shutting down connections
	if n.eventBus != nil && n.mesh != nil {
		logstore.BestEffortPublish(n.logProducer, logstore.PublishOptions{Level: "info", Kind: "node", Message: "node shutting down", Data: map[string]any{"reason": "shutdown"}})
		if err := n.eventBus.Emit(events.NodeLeft(n.nodeName, "shutdown")); err != nil {
			n.logf("[node] warning: failed to emit node.left: %v", err)
		}
		// Flush to ensure event is published before closing
		n.mesh.Conn().Flush()
	}

	if n.discovery != nil {
		n.discovery.Stop()
	}
	if n.logQueryService != nil {
		_ = n.logQueryService.Close()
	}
	if n.logHealthService != nil {
		_ = n.logHealthService.Close()
	}
	if n.logSubscriber != nil {
		_ = n.logSubscriber.Close()
	} else if n.logStore != nil {
		_ = n.logStore.Close()
	}
	if n.executor != nil {
		n.executor.Close()
	}
	if n.mesh != nil {
		n.mesh.Close()
	}
	if n.natsServer != nil {
		n.natsServer.Shutdown()
	}
}

func (n *Node) startLoggingService() error {
	cfg := n.cfg.LoggingService
	if !cfg.Enabled || cfg.Role != "server" || n.mesh == nil {
		return nil
	}
	maxQueryWindow := time.Duration(0)
	if cfg.MaxQueryWindow != "" {
		parsed, err := time.ParseDuration(cfg.MaxQueryWindow)
		if err != nil {
			n.logf("[node] warning: invalid logging_service.max_query_window %q: %v", cfg.MaxQueryWindow, err)
		} else {
			maxQueryWindow = parsed
		}
	}
	store, err := logstore.NewStore(logstore.Config{
		Enabled:        cfg.Enabled,
		Role:           cfg.Role,
		StorageDir:     cfg.StorageDir,
		Subjects:       append([]string(nil), cfg.Subjects...),
		RetentionDays:  cfg.RetentionDays,
		MaxBytes:       cfg.MaxBytes,
		Redact:         cfg.Redact,
		CaptureEvents:  containsSubject(cfg.Subjects, "events.>"),
		MaxQueryLimit:  cfg.MaxQueryLimit,
		MaxQueryWindow: maxQueryWindow,
	})
	if err != nil {
		return err
	}
	sub, err := logstore.Subscribe(n.mesh.Conn(), store)
	if err != nil {
		_ = store.Close()
		return err
	}
	healthSvc, err := store.StartHealthService(n.mesh.Conn(), "logstore.health")
	if err != nil {
		_ = sub.Close()
		return err
	}
	querySvc, err := store.StartQueryServices(n.mesh.Conn())
	if err != nil {
		_ = healthSvc.Close()
		_ = sub.Close()
		return err
	}
	n.logStore = store
	n.logSubscriber = sub
	n.logHealthService = healthSvc
	n.logQueryService = querySvc

	js, err := n.mesh.Conn().JetStream()
	if err != nil {
		n.logf("[node] warning: failed to create logstore state store: %v", err)
	} else {
		stateStore, err := state.NewStore(js)
		if err != nil {
			n.logf("[node] warning: failed to initialize node health store for logstore: %v", err)
		} else {
			n.logStateStore = stateStore
			n.publishLogstoreHealth()
		}
	}
	return nil
}

func (n *Node) publishLogstoreHealth() {
	if n.logStateStore == nil || n.logStore == nil {
		return
	}
	health := n.logStore.Health()
	status := "online"
	if !health.OK {
		status = "degraded"
	}
	extraTools := []string{}
	for _, tool := range n.manager.List() {
		if tool.Status == "running" {
			extraTools = append(extraTools, tool.Name)
		}
	}
	if err := n.logStateStore.SetNodeHealth(state.NodeHealth{
		Node:         n.nodeName,
		Status:       status,
		Arch:         n.cfg.Node.Capabilities.Arch,
		OS:           n.cfg.Node.Capabilities.OS,
		GPU:          n.cfg.Node.Capabilities.GPU != nil && *n.cfg.Node.Capabilities.GPU,
		GPUModel:     n.cfg.Node.Capabilities.GPUModel,
		VRAMGB:       int(n.cfg.Node.Capabilities.VRAMGB),
		CPUCores:     n.cfg.Node.Capabilities.CPUCores,
		MemoryGB:     int(n.cfg.Node.Capabilities.MemoryGB),
		DiskGB:       int(n.cfg.Node.Capabilities.DiskGB),
		ToolCount:    len(n.manager.List()),
		RunningTools: extraTools,
	}); err != nil {
		n.logf("[node] warning: failed to publish logstore health state: %v", err)
	}
}

func containsSubject(subjects []string, want string) bool {
	for _, subject := range subjects {
		if subject == want {
			return true
		}
	}
	return false
}

func levelForCall(err error) string {
	if err != nil {
		return "error"
	}
	return "info"
}

func messageForCall(toolName, method string, err error) string {
	if err != nil {
		return fmt.Sprintf("%s.%s failed", toolName, method)
	}
	return fmt.Sprintf("%s.%s completed", toolName, method)
}
