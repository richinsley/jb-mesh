package config

import (
	"fmt"

	"github.com/richinsley/jb-mesh/pkg/version"
)

// Manifest represents a jumpboot.yaml tool manifest
type Manifest struct {
	Name          string       `yaml:"name"`
	Version       string       `yaml:"version"`
	Description   string       `yaml:"description"`
	Capabilities  []string     `yaml:"capabilities,omitempty"`
	Runtime       Runtime      `yaml:"runtime"`
	Resources     Resources    `yaml:"resources,omitempty"`
	RPC           RPC          `yaml:"rpc"`
	Health        *Health      `yaml:"health,omitempty"`
	Setup         *Setup       `yaml:"setup,omitempty"`
	Config        *ToolConfig  `yaml:"config,omitempty"`
	Deploy        *ReleaseSpec `yaml:"x-deploy,omitempty"`
	ReleaseLegacy *ReleaseSpec `yaml:"x-release,omitempty"`
}

// ReleaseSpec defines portable operator-facing release metadata.
// This is intentionally separate from the runtime contract used by mesh nodes.
type ReleaseSpec struct {
	Smoke *ReleaseCall `yaml:"smoke,omitempty"`
}

// EffectiveRelease returns the deploy/release metadata, preferring x-deploy.
func (m *Manifest) EffectiveRelease() *ReleaseSpec {
	if m.Deploy != nil {
		return m.Deploy
	}
	return m.ReleaseLegacy
}

// ReleaseCall defines a safe method call to use during release/smoke validation.
type ReleaseCall struct {
	Method string                 `yaml:"method,omitempty"`
	Params map[string]interface{} `yaml:"params,omitempty"`
}

// ToolConfig defines configurable parameters for a tool
type ToolConfig struct {
	Schema   map[string]ConfigParam `yaml:"schema"`
	Required []string               `yaml:"required,omitempty"`
}

// ConfigParam defines a single configurable parameter
type ConfigParam struct {
	Type        string        `yaml:"type" json:"type"` // string, int, float, bool
	Default     interface{}   `yaml:"default,omitempty" json:"default,omitempty"`
	Description string        `yaml:"description,omitempty" json:"description,omitempty"`
	Enum        []interface{} `yaml:"enum,omitempty" json:"enum,omitempty"`
}

// Setup defines post-install setup configuration (e.g., model downloads)
type Setup struct {
	Method  string `yaml:"method,omitempty"`  // Method to call, default: "setup"
	Timeout int    `yaml:"timeout,omitempty"` // Seconds to wait, default: 600 (10 min)
}

// Runtime defines the Python environment requirements
type Runtime struct {
	Python         string   `yaml:"python"`                    // Python version (e.g., "3.11")
	Requirements   string   `yaml:"requirements,omitempty"`    // requirements.txt path
	Packages       []string `yaml:"packages,omitempty"`        // pip packages to install
	CondaPackages  []string `yaml:"conda_packages,omitempty"`  // conda packages
	Mode           string   `yaml:"mode"`                      // "persistent" or "oneshot"
	Transport      string   `yaml:"transport,omitempty"`       // "repl" (default) or "msgpack"
	Entrypoint     string   `yaml:"entrypoint,omitempty"`      // main script, defaults to main.py
	StartupTimeout int      `yaml:"startup_timeout,omitempty"` // seconds
}

// Resources defines resource hints for scheduling
type Resources struct {
	GPU    bool `yaml:"gpu,omitempty"`
	VRAMGB int  `yaml:"vram_gb,omitempty"`
	RAMGB  int  `yaml:"ram_gb,omitempty"`
}

// RPC defines the tool's RPC interface
type RPC struct {
	Transport string            `yaml:"transport,omitempty"` // "http" (default) or "jsonqueue"
	Port      interface{}       `yaml:"port,omitempty"`      // "auto" or fixed number
	Methods   map[string]Method `yaml:"methods"`
}

// Method defines a single RPC method
type Method struct {
	Description string  `yaml:"description" json:"description"`
	Input       *Schema `yaml:"input,omitempty" json:"input,omitempty"`
	Output      *Schema `yaml:"output,omitempty" json:"output,omitempty"`

	// Stream marks the method as a streaming endpoint (Phase 2 of
	// DESIGN-STREAMING-CANCEL.md). When true, the node registers a
	// `tools.<tool>.<method>.stream` subject in addition to the regular
	// single-reply subject; callers using Mesh.Stream hit the streaming
	// subject and receive multi-frame responses. Defaults to false.
	Stream bool `yaml:"stream,omitempty" json:"stream,omitempty"`
}

// Schema is a simplified JSON Schema
type Schema struct {
	Type       string             `yaml:"type,omitempty" json:"type,omitempty"`
	Properties map[string]*Schema `yaml:"properties,omitempty" json:"properties,omitempty"`
	Required   []string           `yaml:"required,omitempty" json:"required,omitempty"`
	Items      *Schema            `yaml:"items,omitempty" json:"items,omitempty"`
	Default    interface{}        `yaml:"default,omitempty" json:"default,omitempty"`
	Desc       string             `yaml:"description,omitempty" json:"description,omitempty"`
}

// Health defines health check configuration
type Health struct {
	Method           string `yaml:"method,omitempty"`            // Method to call, default: "health"
	Interval         int    `yaml:"interval,omitempty"`          // Seconds between checks, default: 30
	FailureThreshold int    `yaml:"failure_threshold,omitempty"` // Consecutive failures before unhealthy, default: 3
}

// Validate checks that the manifest has required fields and valid values.
// Returns an error if the manifest is invalid.
func (m *Manifest) Validate() error {
	if m.Name == "" {
		return fmt.Errorf("manifest: name is required")
	}
	if m.Version == "" {
		return fmt.Errorf("manifest %s: version is required", m.Name)
	}
	if err := version.Validate(m.Version); err != nil {
		return fmt.Errorf("manifest %s: %w", m.Name, err)
	}
	return nil
}

// ParsedVersion returns the parsed semver Version from the manifest.
// Call Validate() first to ensure the version is valid.
func (m *Manifest) ParsedVersion() (version.Version, error) {
	return version.Parse(m.Version)
}

// ApplyDefaults fills in sensible defaults
func (m *Manifest) ApplyDefaults() {
	if m.Runtime.Mode == "" {
		m.Runtime.Mode = "oneshot"
	}
	if m.Runtime.Transport == "" {
		m.Runtime.Transport = "repl" // default, can be "msgpack"
	}
	if m.Runtime.Entrypoint == "" {
		m.Runtime.Entrypoint = "main.py"
	}
	if m.Runtime.StartupTimeout == 0 {
		m.Runtime.StartupTimeout = 60
	}
	if m.RPC.Transport == "" {
		m.RPC.Transport = "jsonqueue"
	}
	if m.Health != nil {
		if m.Health.Method == "" {
			m.Health.Method = "health"
		}
		if m.Health.Interval == 0 {
			m.Health.Interval = 30
		}
		if m.Health.FailureThreshold == 0 {
			m.Health.FailureThreshold = 3
		}
	}
	if m.Setup != nil {
		if m.Setup.Method == "" {
			m.Setup.Method = "setup"
		}
		if m.Setup.Timeout == 0 {
			m.Setup.Timeout = 600 // 10 minutes default for model downloads
		}
	}
}
