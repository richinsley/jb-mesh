package tools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/richinsley/jb-mesh/pkg/config"
)

// sharedEnvsDir is created once by TestMain and reused across all tests.
// This avoids re-creating the Python 3.11 base environment (~25s) per test.
var sharedEnvsDir string

func TestMain(m *testing.M) {
	// Create a shared envs directory that persists across all tests.
	// We use os.MkdirTemp instead of t.TempDir() since TestMain has no *testing.T.
	dir, err := os.MkdirTemp("", "jb-mesh-test-envs-*")
	if err != nil {
		panic("failed to create shared envs dir: " + err.Error())
	}
	sharedEnvsDir = dir

	code := m.Run()

	// Cleanup
	os.RemoveAll(sharedEnvsDir)
	os.Exit(code)
}

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.DefaultConfigWithHome(dir)
	// Point EnvsDir at the shared directory so base environments are created once
	cfg.EnvsDir = sharedEnvsDir
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func testJBServicePackage(t *testing.T) string {
	t.Helper()
	if pkg := os.Getenv("JB_MESH_TEST_JB_SERVICE_PACKAGE"); pkg != "" {
		return pkg
	}
	wd, err := os.Getwd()
	if err == nil {
		for dir := wd; ; dir = filepath.Dir(dir) {
			candidate := filepath.Join(dir, "sdk", "python", "jb-service")
			if _, statErr := os.Stat(filepath.Join(candidate, "pyproject.toml")); statErr == nil {
				return candidate
			}
			candidate = filepath.Join(filepath.Dir(dir), "jb-service")
			if _, statErr := os.Stat(filepath.Join(candidate, "pyproject.toml")); statErr == nil {
				return candidate
			}
			if parent := filepath.Dir(dir); parent == dir {
				break
			}
		}
	}
	return "git+https://github.com/richinsley/jb-mesh.git#subdirectory=sdk/python/jb-service"
}

// writeTestTool creates a minimal tool directory with jumpboot.yaml and main.py
func writeTestTool(t *testing.T, dir, name string, persistent bool) string {
	t.Helper()
	toolDir := filepath.Join(dir, name)
	os.MkdirAll(toolDir, 0755)

	mode := "oneshot"
	if persistent {
		mode = "persistent"
	}

	manifest := `name: ` + name + `
version: 1.0.0
description: Test tool
runtime:
  python: "3.11"
  mode: ` + mode + `
  transport: repl
  packages:
    - pydantic>=2.0
    - ` + testJBServicePackage(t) + `
rpc:
  methods:
    echo:
      description: Echo input back
    health:
      description: Health check
`
	os.WriteFile(filepath.Join(toolDir, "jumpboot.yaml"), []byte(manifest), 0644)

	mainPy := `from jb_service import Service, method, run

class TestTool(Service):
    name = "` + name + `"
    version = "1.0.0"

    @method
    def echo(self, message: str = "hello") -> dict:
        return {"echo": message}

    @method
    def health(self) -> dict:
        return {"status": "ok"}

if __name__ == "__main__":
    run(TestTool)
`
	os.WriteFile(filepath.Join(toolDir, "main.py"), []byte(mainPy), 0644)
	return toolDir
}

func TestNewManager(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
}

func TestLoadAllEmpty(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	if err := mgr.LoadAll(); err != nil {
		t.Fatalf("LoadAll on empty dir: %v", err)
	}
	if len(mgr.List()) != 0 {
		t.Fatalf("expected 0 tools, got %d", len(mgr.List()))
	}
}

func TestLoadManifest(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)

	toolDir := writeTestTool(t, cfg.ToolsDir, "echo-tool", false)

	manifest, err := mgr.loadManifest(toolDir)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if manifest.Name != "echo-tool" {
		t.Fatalf("expected name echo-tool, got %s", manifest.Name)
	}
	if manifest.Version != "1.0.0" {
		t.Fatalf("expected version 1.0.0, got %s", manifest.Version)
	}
	if _, ok := manifest.RPC.Methods["echo"]; !ok {
		t.Fatal("expected echo method in manifest")
	}
}

func TestLoadAllWithTool(t *testing.T) {
	cfg := testConfig(t)
	writeTestTool(t, cfg.ToolsDir, "my-tool", false)

	mgr := NewManager(cfg)
	if err := mgr.LoadAll(); err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(mgr.List()) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(mgr.List()))
	}

	tool, ok := mgr.Get("my-tool")
	if !ok {
		t.Fatal("expected to find my-tool")
	}
	if tool.Name != "my-tool" {
		t.Fatalf("expected name my-tool, got %s", tool.Name)
	}
}

func TestGetNonexistent(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	mgr.LoadAll()

	_, ok := mgr.Get("nope")
	if ok {
		t.Fatal("expected not found")
	}
}

func TestInstallLocal(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	mgr.LoadAll()

	// Create a tool in a separate source directory
	srcDir := t.TempDir()
	writeTestTool(t, srcDir, "local-tool", false)
	srcPath := filepath.Join(srcDir, "local-tool")

	tool, err := mgr.Install(srcPath)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if tool.Name != "local-tool" {
		t.Fatalf("expected local-tool, got %s", tool.Name)
	}

	// Should now be in the tools dir
	_, err = os.Stat(filepath.Join(cfg.ToolsDir, "local-tool", "jumpboot.yaml"))
	if err != nil {
		t.Fatalf("tool not installed to tools dir: %v", err)
	}

	// Should be loadable
	got, ok := mgr.Get("local-tool")
	if !ok {
		t.Fatal("installed tool not found via Get")
	}
	if got.Manifest.Version != "1.0.0" {
		t.Fatalf("expected version 1.0.0, got %s", got.Manifest.Version)
	}
}

func TestInstallLocalSubdir(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	mgr.LoadAll()

	repoDir := t.TempDir()
	svcDir := filepath.Join(repoDir, "services", "nested-tool")
	writeTestTool(t, filepath.Join(repoDir, "services"), "nested-tool", false)
	svcDir = filepath.Join(repoDir, "services", "nested-tool")

	tool, err := mgr.Install(svcDir)
	if err != nil {
		t.Fatalf("Install subdir: %v", err)
	}
	if tool.Path != filepath.Join(cfg.ToolsDir, "nested-tool") {
		t.Fatalf("expected installed tool path in tools dir, got %s", tool.Path)
	}
}

func TestInstallDuplicate(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	mgr.LoadAll()

	srcDir := t.TempDir()
	writeTestTool(t, srcDir, "dup-tool", false)
	srcPath := filepath.Join(srcDir, "dup-tool")

	_, err := mgr.Install(srcPath)
	if err != nil {
		t.Fatal(err)
	}

	// Second install should fail
	writeTestTool(t, srcDir+"2", "dup-tool", false)
	_, err = mgr.Install(filepath.Join(srcDir+"2", "dup-tool"))
	if err == nil {
		t.Fatal("expected error installing duplicate tool")
	}
}

func TestUpdatePullsSymlinkTargetGitCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	cfg := testConfig(t)
	mgr := NewManager(cfg)

	remote := filepath.Join(t.TempDir(), "remote.git")
	runTestGit(t, "", "init", "--bare", remote)
	work := filepath.Join(t.TempDir(), "work")
	runTestGit(t, "", "clone", remote, work)
	runTestGit(t, work, "config", "user.email", "test@example.com")
	runTestGit(t, work, "config", "user.name", "Test")

	svcDir := filepath.Join(work, "services", "demo")
	writeTestTool(t, filepath.Join(work, "services"), "demo", false)
	runTestGit(t, work, "add", ".")
	runTestGit(t, work, "commit", "-m", "initial")
	runTestGit(t, work, "push", "origin", "HEAD:main")
	runTestGit(t, work, "branch", "--set-upstream-to", "origin/main")

	tool, err := mgr.Install(svcDir)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	other := filepath.Join(t.TempDir(), "other")
	runTestGit(t, "", "clone", remote, other)
	runTestGit(t, other, "config", "user.email", "test@example.com")
	runTestGit(t, other, "config", "user.name", "Test")
	manifestPath := filepath.Join(other, "services", "demo", "jumpboot.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "version: 1.0.0", "version: 1.1.0", 1))
	if err := os.WriteFile(manifestPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, other, "add", ".")
	runTestGit(t, other, "commit", "-m", "bump")
	runTestGit(t, other, "push", "origin", "HEAD:main")

	updated, err := mgr.Update(tool.Name, false)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Manifest.Version != "1.1.0" {
		t.Fatalf("expected updated manifest version 1.1.0, got %s", updated.Manifest.Version)
	}
}

func runTestGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}
