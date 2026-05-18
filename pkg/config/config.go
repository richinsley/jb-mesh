package config

import (
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

// Config is the global jb-mesh configuration
type Config struct {
	HomeDir   string `yaml:"-"`          // Base directory (not saved to yaml)
	ToolsDir  string `yaml:"tools_dir"`  // Where tools are installed
	EnvsDir   string `yaml:"envs_dir"`   // Where jumpboot environments live
	RunDir    string `yaml:"run_dir"`    // Runtime state (pids, sockets)
	APIPort   int    `yaml:"api_port"`   // Default API server port
	AuthToken string `yaml:"auth_token"` // Optional auth token

	// Node identity and capabilities
	Node NodeConfig `yaml:"node"`

	// NATS connection settings
	NATS NATSConfig `yaml:"nats"`

	// Security policy
	Security SecurityConfig `yaml:"security"`

	// Logging
	Logging LoggingConfig `yaml:"logging"`

	// Discovery (mDNS zero-conf)
	Discovery DiscoveryConfig `yaml:"discovery"`

	// Logging service (durable mesh log store)
	LoggingService LoggingServiceConfig `yaml:"logging_service"`
}

// NodeConfig holds identity and capability information for this node.
type NodeConfig struct {
	// Name is a human-readable unique name for this node in the mesh.
	Name string `yaml:"name,omitempty"`

	// Role is an informational hint: "worker", "gateway", "storage".
	// Not enforced by the mesh — purely for discovery and scheduling.
	Role string `yaml:"role,omitempty"`

	// Capabilities declares what this node can do.
	// Auto-detected values are used when not explicitly set.
	Capabilities Capabilities `yaml:"capabilities"`
}

// Capabilities describes the resources available on a node.
// Fields set in config.yaml override auto-detected values.
type Capabilities struct {
	GPU      *bool   `yaml:"gpu,omitempty"`       // Has a GPU (auto-detected on Linux via nvidia-smi)
	GPUModel string  `yaml:"gpu_model,omitempty"` // e.g., "RTX 3090"
	VRAMGB   float64 `yaml:"vram_gb,omitempty"`   // GPU memory in GB
	CPUCores int     `yaml:"cpu_cores,omitempty"` // Number of logical CPU cores (auto-detected)
	MemoryGB float64 `yaml:"memory_gb,omitempty"` // System RAM in GB
	DiskGB   float64 `yaml:"disk_gb,omitempty"`   // Available disk in GB
	Arch     string  `yaml:"arch,omitempty"`      // CPU architecture (auto-detected: amd64, arm64)
	OS       string  `yaml:"os,omitempty"`        // Operating system (auto-detected: linux, darwin, windows)
}

// NATSConfig holds NATS connection settings.
type NATSConfig struct {
	// URL is the NATS server address. Default: nats://localhost:4222
	URL string `yaml:"url,omitempty"`

	// Token is the auth token for NATS. Overrides Config.AuthToken if set.
	Token string `yaml:"token,omitempty"`

	// Embed starts an embedded NATS server on this node.
	Embed bool `yaml:"embed,omitempty"`

	// EmbedPort is the port for the embedded NATS server. Default: 4222.
	EmbedPort int `yaml:"embed_port,omitempty"`

	// JetStream controls whether JetStream is enabled. Default: true.
	// Set to false only for resource-constrained edge nodes.
	JetStream *bool `yaml:"jetstream,omitempty"`
}

// JetStreamEnabled returns whether JetStream should be enabled.
// Default is true (DESIGN.md §14, Q1).
func (n *NATSConfig) JetStreamEnabled() bool {
	if n.JetStream == nil {
		return true
	}
	return *n.JetStream
}

// SecurityConfig holds security policy settings.
type SecurityConfig struct {
	// InstallPolicy controls who can install tools on this node.
	// Values: "open" (default), "restricted", "approval"
	InstallPolicy string `yaml:"install_policy,omitempty"`

	// AllowedSources is a list of allowed git URL patterns (for restricted mode).
	AllowedSources []string `yaml:"allowed_sources,omitempty"`

	// AllowedInstallers is a list of node names allowed to install (for restricted mode).
	AllowedInstallers []string `yaml:"allowed_installers,omitempty"`
}

// DiscoveryConfig holds mDNS discovery settings.
type DiscoveryConfig struct {
	// MDNS enables mDNS-based peer discovery on the LAN. Default: true.
	MDNS bool `yaml:"mdns"`

	// Cluster enables auto-clustering with discovered peers. Default: true.
	Cluster bool `yaml:"cluster"`

	// ClusterPort is the NATS cluster port. Default: client_port + 1000.
	ClusterPort int `yaml:"cluster_port,omitempty"`
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	// Level is the minimum log level: debug, info, warn, error. Default: info.
	Level string `yaml:"level,omitempty"`

	// File is an optional path to write logs to (in addition to stderr).
	File string `yaml:"file,omitempty"`

	// JSON enables JSON-formatted log output.
	JSON bool `yaml:"json,omitempty"`
}

// LoggingServiceConfig holds durable mesh logging service settings.
type LoggingServiceConfig struct {
	Enabled        bool     `yaml:"enabled,omitempty"`
	Role           string   `yaml:"role,omitempty"`
	StorageDir     string   `yaml:"storage_dir,omitempty"`
	Subjects       []string `yaml:"subjects,omitempty"`
	RetentionDays  int      `yaml:"retention_days,omitempty"`
	MaxBytes       int64    `yaml:"max_bytes,omitempty"`
	Redact         bool     `yaml:"redact,omitempty"`
	MaxQueryLimit  int      `yaml:"max_query_limit,omitempty"`
	MaxQueryWindow string   `yaml:"max_query_window,omitempty"`
}

// AutoDetectCapabilities fills in capabilities that can be determined
// from the runtime. Explicit config values are never overwritten.
func (c *Capabilities) AutoDetectCapabilities() {
	if c.Arch == "" {
		c.Arch = runtime.GOARCH
	}
	if c.OS == "" {
		c.OS = runtime.GOOS
	}
	if c.CPUCores == 0 {
		c.CPUCores = runtime.NumCPU()
	}
}

// GetHomeDir returns the jb-mesh home directory.
// Priority: explicit homeDir param > JB_SERVE_HOME env > ~/.jb-mesh
func GetHomeDir(homeDir string) string {
	if homeDir != "" {
		return homeDir
	}
	if envHome := os.Getenv("JB_SERVE_HOME"); envHome != "" {
		return envHome
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".jb-mesh")
}

// DefaultConfig returns config with default paths
func DefaultConfig() *Config {
	return DefaultConfigWithHome("")
}

// DefaultConfigWithHome returns config with paths based on the specified home directory
func DefaultConfigWithHome(homeDir string) *Config {
	base := GetHomeDir(homeDir)

	return &Config{
		HomeDir:  base,
		ToolsDir: filepath.Join(base, "tools"),
		EnvsDir:  filepath.Join(base, "envs"),
		RunDir:   filepath.Join(base, "run"),
		APIPort:  9800,
		Discovery: DiscoveryConfig{
			MDNS:    true,
			Cluster: true,
		},
	}
}

// BaseDir returns the jb-mesh base directory
func (c *Config) BaseDir() string {
	if c.HomeDir != "" {
		return c.HomeDir
	}
	return GetHomeDir("")
}

// ConfigPath returns the path to the config file
func (c *Config) ConfigPath() string {
	return filepath.Join(c.BaseDir(), "config.yaml")
}

// Load reads config from disk, or returns defaults
func Load() (*Config, error) {
	return LoadWithHome("")
}

// LoadWithHome reads config from disk using the specified home directory
func LoadWithHome(homeDir string) (*Config, error) {
	cfg := DefaultConfigWithHome(homeDir)

	data, err := os.ReadFile(cfg.ConfigPath())
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // Use defaults
		}
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// Preserve HomeDir after loading from yaml (it's not in yaml)
	cfg.HomeDir = GetHomeDir(homeDir)

	return cfg, nil
}

// Save writes config to disk
func (c *Config) Save() error {
	if err := os.MkdirAll(c.BaseDir(), 0755); err != nil {
		return err
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}

	return os.WriteFile(c.ConfigPath(), data, 0644)
}

// JetStreamDir returns the directory used for JetStream persistence (streams, KV, object store).
func (c *Config) JetStreamDir() string {
	return filepath.Join(c.BaseDir(), "jetstream")
}

// EnsureDirs creates all necessary directories
func (c *Config) EnsureDirs() error {
	dirs := []string{c.ToolsDir, c.EnvsDir, c.RunDir, c.JetStreamDir()}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}
