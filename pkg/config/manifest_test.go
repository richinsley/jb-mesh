package config

import (
	"testing"

	"github.com/richinsley/jb-mesh/pkg/version"
)

func TestApplyDefaults_Empty(t *testing.T) {
	m := &Manifest{}
	m.ApplyDefaults()

	if m.Runtime.Mode != "oneshot" {
		t.Fatalf("expected mode oneshot, got %s", m.Runtime.Mode)
	}
	if m.Runtime.Transport != "repl" {
		t.Fatalf("expected transport repl, got %s", m.Runtime.Transport)
	}
	if m.Runtime.Entrypoint != "main.py" {
		t.Fatalf("expected entrypoint main.py, got %s", m.Runtime.Entrypoint)
	}
	if m.Runtime.StartupTimeout != 60 {
		t.Fatalf("expected startup_timeout 60, got %d", m.Runtime.StartupTimeout)
	}
}

func TestApplyDefaults_PreservesExisting(t *testing.T) {
	m := &Manifest{
		Runtime: Runtime{
			Mode:           "persistent",
			Transport:      "msgpack",
			Entrypoint:     "server.py",
			StartupTimeout: 120,
		},
	}
	m.ApplyDefaults()

	if m.Runtime.Mode != "persistent" {
		t.Fatalf("expected persistent, got %s", m.Runtime.Mode)
	}
	if m.Runtime.Transport != "msgpack" {
		t.Fatalf("expected msgpack, got %s", m.Runtime.Transport)
	}
	if m.Runtime.Entrypoint != "server.py" {
		t.Fatalf("expected server.py, got %s", m.Runtime.Entrypoint)
	}
}

func TestApplyDefaults_Health(t *testing.T) {
	m := &Manifest{
		Health: &Health{},
	}
	m.ApplyDefaults()

	if m.Health.Method != "health" {
		t.Fatalf("expected health method 'health', got %s", m.Health.Method)
	}
	if m.Health.Interval != 30 {
		t.Fatalf("expected interval 30, got %d", m.Health.Interval)
	}
	if m.Health.FailureThreshold != 3 {
		t.Fatalf("expected threshold 3, got %d", m.Health.FailureThreshold)
	}
}

func TestApplyDefaults_Setup(t *testing.T) {
	m := &Manifest{
		Setup: &Setup{},
	}
	m.ApplyDefaults()

	if m.Setup.Method != "setup" {
		t.Fatalf("expected setup method 'setup', got %s", m.Setup.Method)
	}
	if m.Setup.Timeout != 600 {
		t.Fatalf("expected timeout 600, got %d", m.Setup.Timeout)
	}
}

func TestApplyDefaults_NilHealth(t *testing.T) {
	m := &Manifest{}
	m.ApplyDefaults()
	if m.Health != nil {
		t.Fatal("Health should remain nil if not set")
	}
}

// --- Validate ---

func TestValidate_Valid(t *testing.T) {
	m := &Manifest{Name: "test-tool", Version: "v1.2.3"}
	if err := m.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_MissingName(t *testing.T) {
	m := &Manifest{Version: "v1.0.0"}
	if err := m.Validate(); err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestValidate_MissingVersion(t *testing.T) {
	m := &Manifest{Name: "test-tool"}
	if err := m.Validate(); err == nil {
		t.Fatal("expected error for missing version")
	}
}

func TestValidate_InvalidVersion(t *testing.T) {
	tests := []string{
		"1.2.3",      // missing v prefix
		"v1.2",       // incomplete
		"latest",     // not semver
		"v1.2.3-rc1", // pre-release not supported
	}
	for _, ver := range tests {
		t.Run(ver, func(t *testing.T) {
			m := &Manifest{Name: "test-tool", Version: ver}
			if err := m.Validate(); err == nil {
				t.Fatalf("expected error for version %q", ver)
			}
		})
	}
}

func TestParsedVersion(t *testing.T) {
	m := &Manifest{Name: "test-tool", Version: "v1.2.3"}
	v, err := m.ParsedVersion()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := version.Version{Major: 1, Minor: 2, Patch: 3}
	if v != want {
		t.Fatalf("got %v, want %v", v, want)
	}
}
