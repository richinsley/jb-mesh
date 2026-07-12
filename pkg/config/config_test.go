package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ToolsDir == "" {
		t.Fatal("ToolsDir should not be empty")
	}
	if cfg.EnvsDir == "" {
		t.Fatal("EnvsDir should not be empty")
	}
	if cfg.APIPort != 9800 {
		t.Fatalf("expected default APIPort 9800, got %d", cfg.APIPort)
	}
}

func TestDefaultConfigWithHome(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfigWithHome(dir)
	if cfg.HomeDir != dir {
		t.Fatalf("expected HomeDir %s, got %s", dir, cfg.HomeDir)
	}
	if cfg.ToolsDir != filepath.Join(dir, "tools") {
		t.Fatalf("expected ToolsDir %s, got %s", filepath.Join(dir, "tools"), cfg.ToolsDir)
	}
}

func TestGetHomeDir_Explicit(t *testing.T) {
	got := GetHomeDir("/explicit/path")
	if got != "/explicit/path" {
		t.Fatalf("expected /explicit/path, got %s", got)
	}
}

func TestGetHomeDir_Env(t *testing.T) {
	t.Setenv("JB_SERVE_HOME", "/env/path")
	got := GetHomeDir("")
	if got != "/env/path" {
		t.Fatalf("expected /env/path, got %s", got)
	}
}

func TestGetHomeDir_Default(t *testing.T) {
	t.Setenv("JB_SERVE_HOME", "")
	got := GetHomeDir("")
	home, _ := os.UserHomeDir()
	expected := filepath.Join(home, ".jb-mesh")
	if got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}

func TestEnsureDirs(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfigWithHome(dir)
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs failed: %v", err)
	}
	for _, d := range []string{cfg.ToolsDir, cfg.EnvsDir, cfg.RunDir} {
		if _, err := os.Stat(d); os.IsNotExist(err) {
			t.Fatalf("directory not created: %s", d)
		}
	}
}

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfigWithHome(dir)
	cfg.APIPort = 1234
	cfg.AuthToken = "test-token"
	cfg.NATS.EmbedHost = "127.0.0.1"
	cfg.NATS.LeafHost = "127.0.0.1"
	if err := cfg.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := LoadWithHome(dir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.APIPort != 1234 {
		t.Fatalf("expected APIPort 1234, got %d", loaded.APIPort)
	}
	if loaded.AuthToken != "test-token" {
		t.Fatalf("expected AuthToken test-token, got %s", loaded.AuthToken)
	}
	if loaded.NATS.EmbedHost != "127.0.0.1" || loaded.NATS.LeafHost != "127.0.0.1" {
		t.Fatalf("expected NATS bind hosts to round-trip, got embed=%q leaf=%q", loaded.NATS.EmbedHost, loaded.NATS.LeafHost)
	}
}

func TestLoadMissing(t *testing.T) {
	dir := t.TempDir()
	cfg, err := LoadWithHome(dir)
	if err != nil {
		t.Fatalf("Load from empty dir should not error: %v", err)
	}
	if cfg.APIPort != 9800 {
		t.Fatalf("expected default APIPort, got %d", cfg.APIPort)
	}
}

func TestJetStreamDir(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfigWithHome(dir)
	want := filepath.Join(dir, "jetstream")
	if got := cfg.JetStreamDir(); got != want {
		t.Fatalf("JetStreamDir() = %s, want %s", got, want)
	}
}

func TestEnsureDirs_CreatesJetStreamDir(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfigWithHome(dir)
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs failed: %v", err)
	}
	jsDir := cfg.JetStreamDir()
	if _, err := os.Stat(jsDir); os.IsNotExist(err) {
		t.Fatalf("JetStream directory not created: %s", jsDir)
	}
}

func TestAutoDetectCapabilities(t *testing.T) {
	caps := Capabilities{}
	caps.AutoDetectCapabilities()

	if caps.Arch == "" {
		t.Fatal("Arch should be auto-detected")
	}
	if caps.OS == "" {
		t.Fatal("OS should be auto-detected")
	}
	if caps.CPUCores == 0 {
		t.Fatal("CPUCores should be auto-detected")
	}
}

func TestAutoDetectCapabilities_PreservesExplicit(t *testing.T) {
	caps := Capabilities{
		Arch:     "mips",
		OS:       "plan9",
		CPUCores: 42,
	}
	caps.AutoDetectCapabilities()

	if caps.Arch != "mips" {
		t.Fatalf("Arch should be preserved, got %s", caps.Arch)
	}
	if caps.OS != "plan9" {
		t.Fatalf("OS should be preserved, got %s", caps.OS)
	}
	if caps.CPUCores != 42 {
		t.Fatalf("CPUCores should be preserved, got %d", caps.CPUCores)
	}
}

func TestJetStreamEnabled_Default(t *testing.T) {
	nats := NATSConfig{}
	if !nats.JetStreamEnabled() {
		t.Fatal("JetStream should be enabled by default")
	}
}

func TestJetStreamEnabled_Disabled(t *testing.T) {
	f := false
	nats := NATSConfig{JetStream: &f}
	if nats.JetStreamEnabled() {
		t.Fatal("JetStream should be disabled when explicitly set to false")
	}
}

func TestNodeConfig_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfigWithHome(dir)
	cfg.Node.Name = "test-node"
	cfg.Node.Role = "worker"
	cfg.Node.Capabilities.GPUModel = "RTX 3090"
	cfg.Node.Capabilities.VRAMGB = 24
	cfg.Security.InstallPolicy = "restricted"
	cfg.Security.AllowedSources = []string{"http://gogs/*"}
	cfg.Logging.Level = "debug"
	cfg.LoggingService = LoggingServiceConfig{
		Enabled:        true,
		Role:           "server",
		StorageDir:     "/var/lib/jb-mesh/logstore",
		Subjects:       []string{"logs.>", "events.>"},
		RetentionDays:  180,
		MaxBytes:       2048,
		Redact:         true,
		MaxQueryLimit:  1000,
		MaxQueryWindow: "168h",
	}

	if err := cfg.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := LoadWithHome(dir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.Node.Name != "test-node" {
		t.Fatalf("Node.Name = %s, want test-node", loaded.Node.Name)
	}
	if loaded.Node.Role != "worker" {
		t.Fatalf("Node.Role = %s, want worker", loaded.Node.Role)
	}
	if loaded.Node.Capabilities.GPUModel != "RTX 3090" {
		t.Fatalf("GPUModel = %s, want RTX 3090", loaded.Node.Capabilities.GPUModel)
	}
	if loaded.Node.Capabilities.VRAMGB != 24 {
		t.Fatalf("VRAMGB = %f, want 24", loaded.Node.Capabilities.VRAMGB)
	}
	if loaded.Security.InstallPolicy != "restricted" {
		t.Fatalf("InstallPolicy = %s, want restricted", loaded.Security.InstallPolicy)
	}
	if len(loaded.Security.AllowedSources) != 1 || loaded.Security.AllowedSources[0] != "http://gogs/*" {
		t.Fatalf("AllowedSources = %v, want [http://gogs/*]", loaded.Security.AllowedSources)
	}
	if loaded.Logging.Level != "debug" {
		t.Fatalf("Logging.Level = %s, want debug", loaded.Logging.Level)
	}
	if !loaded.LoggingService.Enabled || loaded.LoggingService.Role != "server" {
		t.Fatalf("LoggingService = %+v, want enabled server", loaded.LoggingService)
	}
	if loaded.LoggingService.StorageDir != "/var/lib/jb-mesh/logstore" {
		t.Fatalf("LoggingService.StorageDir = %s", loaded.LoggingService.StorageDir)
	}
	if len(loaded.LoggingService.Subjects) != 2 || loaded.LoggingService.Subjects[1] != "events.>" {
		t.Fatalf("LoggingService.Subjects = %v", loaded.LoggingService.Subjects)
	}
	if loaded.LoggingService.MaxQueryWindow != "168h" {
		t.Fatalf("LoggingService.MaxQueryWindow = %s", loaded.LoggingService.MaxQueryWindow)
	}
}
