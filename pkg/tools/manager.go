package tools

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/richinsley/jb-mesh/pkg/config"
	"github.com/richinsley/jb-mesh/pkg/sdk"
	"github.com/richinsley/jumpboot"
	"gopkg.in/yaml.v3"
)

// Tool represents an installed tool with its jumpboot environment
type Tool struct {
	Name           string                      `json:"name"`
	Path           string                      `json:"path"` // Filesystem path to tool repo
	Manifest       *config.Manifest            `json:"manifest"`
	Env            *jumpboot.PythonEnvironment `json:"-"`               // Jumpboot environment
	Status         string                      `json:"status"`          // "stopped", "running"
	HealthStatus   string                      `json:"health_status"`   // "healthy", "unhealthy", "unknown"
	HealthFailures int                         `json:"health_failures"` // Consecutive health check failures
	PID            int                         `json:"pid,omitempty"`
	Port           int                         `json:"port,omitempty"`
}

// Manager handles tool lifecycle using jumpboot
type Manager struct {
	cfg   *config.Config
	tools map[string]*Tool
}

// NewManager creates a new tool manager
func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		cfg:   cfg,
		tools: make(map[string]*Tool),
	}
}

// LoadAll scans the tools directory and loads all manifests
func (m *Manager) LoadAll() error {
	entries, err := os.ReadDir(m.cfg.ToolsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		toolPath := filepath.Join(m.cfg.ToolsDir, entry.Name())

		// Follow symlinks
		info, err := os.Stat(toolPath)
		if err != nil || !info.IsDir() {
			continue
		}

		manifest, err := m.loadManifest(toolPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to load %s: %v\n", entry.Name(), err)
			continue
		}

		m.tools[manifest.Name] = &Tool{
			Name:     manifest.Name,
			Path:     toolPath,
			Manifest: manifest,
			Status:   "stopped",
		}
	}

	return nil
}

// loadManifest reads and parses a jumpboot.yaml
func (m *Manager) loadManifest(toolPath string) (*config.Manifest, error) {
	toolPath, err := ResolveToolSource(toolPath)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(toolPath, "jumpboot.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}

	var manifest config.Manifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	manifest.ApplyDefaults()
	return &manifest, nil
}

// Install installs a tool from git URL or local path
func (m *Manager) Install(source string) (*Tool, error) {
	var toolPath string
	var err error

	// Determine if source is local or git
	if strings.HasPrefix(source, "/") || strings.HasPrefix(source, "./") || strings.HasPrefix(source, "~") {
		toolPath, err = m.installLocal(source)
	} else {
		toolPath, err = m.installGit(source)
	}
	if err != nil {
		return nil, err
	}

	// Load manifest
	manifest, err := m.loadManifest(toolPath)
	if err != nil {
		os.RemoveAll(toolPath)
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}

	// Check if already installed
	if existing, ok := m.tools[manifest.Name]; ok {
		os.RemoveAll(toolPath)
		return nil, fmt.Errorf("tool %s already installed at %s", manifest.Name, existing.Path)
	}

	// Create jumpboot environment
	fmt.Printf("Creating Python %s environment for %s...\n", manifest.Runtime.Python, manifest.Name)
	env, err := m.createEnvironment(manifest)
	if err != nil {
		os.RemoveAll(toolPath)
		return nil, fmt.Errorf("failed to create environment: %w", err)
	}

	// Always install packages on explicit install (no cache — fresh from origin)
	if err := m.installPackages(env, manifest, toolPath, true); err != nil {
		return nil, fmt.Errorf("failed to install packages: %w", err)
	}

	tool := &Tool{
		Name:     manifest.Name,
		Path:     toolPath,
		Manifest: manifest,
		Env:      env,
		Status:   "stopped",
	}

	// Run setup if defined (downloads models, etc.)
	if manifest.Setup != nil {
		fmt.Printf("Running setup for %s (this may take a while for model downloads)...\n", manifest.Name)
		if err := m.runSetup(tool); err != nil {
			os.RemoveAll(toolPath)
			return nil, fmt.Errorf("setup failed: %w", err)
		}
		fmt.Printf("Setup complete for %s\n", manifest.Name)
	}

	m.tools[manifest.Name] = tool

	fmt.Printf("Installed %s v%s\n", manifest.Name, manifest.Version)
	return tool, nil
}

// getOrCreateBase returns (or creates) the shared base mamba environment for a Python version.
// Base envs live at <envsDir>/base-<version> and are reused across all tools needing that version.
func (m *Manager) getOrCreateBase(pythonVersion string) (*jumpboot.PythonEnvironment, error) {
	baseName := fmt.Sprintf("base-%s", pythonVersion)

	env, err := jumpboot.CreateEnvironmentMamba(
		baseName,
		m.cfg.EnvsDir,
		pythonVersion,
		"conda-forge",
		nil, // progress callback
	)
	if err != nil {
		return nil, err
	}

	if env.IsNew {
		fmt.Printf("Created base Python %s environment\n", pythonVersion)
	}

	return env, nil
}

// createEnvironment creates a lightweight venv for a tool on top of a shared base env.
// Layout:
//
//	<envsDir>/base-3.11/       ← full mamba install (shared)
//	<envsDir>/venvs/calculator/ ← lightweight venv (per-tool site-packages only)
func (m *Manager) createEnvironment(manifest *config.Manifest) (*jumpboot.PythonEnvironment, error) {
	// Get or create the shared base for this Python version
	baseEnv, err := m.getOrCreateBase(manifest.Runtime.Python)
	if err != nil {
		return nil, fmt.Errorf("failed to create base env for Python %s: %w", manifest.Runtime.Python, err)
	}

	// Create a lightweight venv on top of the base
	venvPath := filepath.Join(m.cfg.EnvsDir, "venvs", manifest.Name)

	env, err := jumpboot.CreateVenvEnvironment(
		baseEnv,
		venvPath,
		jumpboot.VenvOptions{}, // default options
		nil,                    // progress callback
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create venv for %s: %w", manifest.Name, err)
	}

	return env, nil
}

// installPackages installs pip/conda packages into the environment.
// When noCache is true, pip skips its cache — needed for clean rebuilds
// so that git+ dependencies are re-fetched from origin.
func (m *Manager) installPackages(env *jumpboot.PythonEnvironment, manifest *config.Manifest, toolPath string, noCache bool) error {
	// Install conda packages first (one at a time via micromamba)
	if len(manifest.Runtime.CondaPackages) > 0 {
		fmt.Printf("Installing conda packages: %v\n", manifest.Runtime.CondaPackages)
		for _, pkg := range manifest.Runtime.CondaPackages {
			if err := env.MicromambaInstallPackage(pkg, "conda-forge"); err != nil {
				return err
			}
		}
	}

	// Install pip packages
	if len(manifest.Runtime.Packages) > 0 {
		packages := sdk.RewritePythonSDKPackages(manifest.Runtime.Packages)
		fmt.Printf("Installing pip packages: %v\n", packages)
		if err := env.PipInstallPackages(packages, "", "", noCache, nil); err != nil {
			return err
		}
	}

	// Install from requirements.txt
	if manifest.Runtime.Requirements != "" {
		reqPath := filepath.Join(toolPath, manifest.Runtime.Requirements)
		if _, err := os.Stat(reqPath); err == nil {
			fmt.Printf("Installing from %s\n", manifest.Runtime.Requirements)
			if err := env.PipInstallRequirements(reqPath, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

// runSetup runs the tool's setup method to download models, warm caches, etc.
func (m *Manager) runSetup(tool *Tool) error {
	setupCfg := tool.Manifest.Setup
	method := setupCfg.Method
	timeout := setupCfg.Timeout

	// Use msgpack transport if specified, otherwise REPL
	transport := tool.Manifest.Runtime.Transport
	if transport == "msgpack" {
		return m.runSetupMsgpack(tool, method, timeout)
	}
	return m.runSetupRepl(tool, method, timeout)
}

// runSetupRepl runs setup using REPL transport
func (m *Manager) runSetupRepl(tool *Tool, method string, timeout int) error {
	entrypoint := filepath.Join(tool.Path, tool.Manifest.Runtime.Entrypoint)

	// Create a REPL process
	repl, err := tool.Env.NewREPLPythonProcess(nil, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create REPL: %w", err)
	}
	defer repl.Close()

	// Initialize the service
	initCode := fmt.Sprintf(`
__name__ = "__main__"
exec(open(%q).read())
`, entrypoint)

	_, err = repl.Execute(initCode, true)
	if err != nil {
		return fmt.Errorf("failed to run entrypoint: %w", err)
	}

	// Import jb functions
	importCode := `
import builtins
if hasattr(builtins, '__jb_call__'):
    __jb_call__ = builtins.__jb_call__
`
	_, err = repl.Execute(importCode, true)
	if err != nil {
		return fmt.Errorf("failed to import jb functions: %w", err)
	}

	// Call the setup method
	callCode := fmt.Sprintf(`__jb_call__(%q, {})`, method)
	result, err := repl.Execute(callCode, true)
	if err != nil {
		return fmt.Errorf("setup call failed: %w", err)
	}

	// Check for errors in response
	if strings.Contains(result, `"ok": false`) || strings.Contains(result, `"ok":false`) {
		return fmt.Errorf("setup returned error: %s", result)
	}

	return nil
}

// runSetupMsgpack runs setup using MessagePack transport
func (m *Manager) runSetupMsgpack(tool *Tool, method string, timeout int) error {
	entrypoint := filepath.Join(tool.Path, tool.Manifest.Runtime.Entrypoint)

	// Create module from entrypoint
	mainModule, err := jumpboot.NewModuleFromPath("__main__", entrypoint)
	if err != nil {
		return fmt.Errorf("failed to load entrypoint: %w", err)
	}

	// Create program
	program := &jumpboot.PythonProgram{
		Name:    tool.Name,
		Path:    tool.Path,
		Program: *mainModule,
	}

	// Create queue process
	queue, err := tool.Env.NewQueueProcess(program, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create queue process: %w", err)
	}
	defer queue.Close()

	// Call the setup method with extended timeout
	_, err = queue.Call(method, timeout, map[string]interface{}{})
	if err != nil {
		return fmt.Errorf("setup call failed: %w", err)
	}

	return nil
}

// installLocal symlinks a local tool
func (m *Manager) installLocal(source string) (string, error) {
	absSource, err := ResolveToolSource(source)
	if err != nil {
		return "", err
	}

	// Load manifest to get name
	manifest, err := m.loadManifest(absSource)
	if err != nil {
		return "", err
	}

	toolPath := filepath.Join(m.cfg.ToolsDir, manifest.Name)
	os.RemoveAll(toolPath)

	if err := os.Symlink(absSource, toolPath); err != nil {
		return "", fmt.Errorf("failed to create symlink: %w", err)
	}

	return toolPath, nil
}

// installGit clones a tool from git
func (m *Manager) installGit(source string) (string, error) {
	gitURL := source
	if !strings.HasPrefix(gitURL, "https://") && !strings.HasPrefix(gitURL, "http://") && !strings.HasPrefix(gitURL, "git@") && !strings.HasPrefix(gitURL, "ssh://") {
		gitURL = "https://" + source
		if !strings.Contains(gitURL, ".git") {
			gitURL += ".git"
		}
	}

	tempDir, err := os.MkdirTemp("", "jb-mesh-install-")
	if err != nil {
		return "", err
	}

	fmt.Printf("Cloning %s...\n", gitURL)
	cmd := exec.Command("git", "clone", "--depth", "1", gitURL, tempDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("git clone failed: %w", err)
	}

	manifest, err := m.loadManifest(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", err
	}

	toolPath := filepath.Join(m.cfg.ToolsDir, manifest.Name)
	os.RemoveAll(toolPath)

	if err := moveDir(tempDir, toolPath); err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to move tool: %w", err)
	}

	return toolPath, nil
}

// moveDir moves a directory, falling back to copy+delete for cross-filesystem moves
func moveDir(src, dst string) error {
	// Try rename first (fast, same filesystem)
	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	// Fall back to copy + delete (cross-filesystem)
	if err := copyDir(src, dst); err != nil {
		return err
	}
	return os.RemoveAll(src)
}

// copyDir recursively copies a directory
func copyDir(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// copyFile copies a single file
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// Update updates a tool's source (git pull or re-copy for local), reloads the manifest,
// and reinstalls packages if they changed. Returns the updated tool.
// The caller is responsible for stopping/restarting the tool if it was running.
func (m *Manager) Update(name string, clean bool) (*Tool, error) {
	tool, ok := m.tools[name]
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", name)
	}

	oldManifest := tool.Manifest

	// Determine if the tool is a symlink (local) or a git clone.
	// Local installs are symlinked into ToolsDir. If the symlink target lives in a
	// git checkout, still pull that checkout so managed remote updates advance
	// repo-subdir services instead of only reloading the old working tree.
	linkTarget, err := os.Readlink(tool.Path)
	if err == nil {
		if err := m.gitPullIfCheckout(linkTarget); err != nil {
			return nil, fmt.Errorf("git pull symlink target failed: %w", err)
		}
	} else {
		// Not a symlink — try git pull in the installed checkout.
		if err := m.gitPull(tool.Path); err != nil {
			return nil, fmt.Errorf("git pull failed: %w", err)
		}
	}

	// Reload manifest
	newManifest, err := m.loadManifest(tool.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to reload manifest: %w", err)
	}

	// Ensure the tool name hasn't changed
	if newManifest.Name != oldManifest.Name {
		return nil, fmt.Errorf("tool name changed from %q to %q — not allowed during update", oldManifest.Name, newManifest.Name)
	}

	tool.Manifest = newManifest

	// Reinstall packages if they changed or clean flag is set
	if clean || m.packagesChanged(oldManifest, newManifest) {
		fmt.Printf("Updating packages for %s...\n", name)

		env, err := m.createEnvironment(newManifest)
		if err != nil {
			return nil, fmt.Errorf("failed to create environment: %w", err)
		}

		if err := m.installPackages(env, newManifest, tool.Path, clean); err != nil {
			return nil, fmt.Errorf("failed to install packages: %w", err)
		}

		tool.Env = env
	}

	fmt.Printf("Updated %s: v%s → v%s\n", name, oldManifest.Version, newManifest.Version)
	return tool, nil
}

// gitPull runs git pull in a directory.
func (m *Manager) gitPull(dir string) error {
	cmd := exec.Command("git", "-C", dir, "pull", "--ff-only")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// gitPullIfCheckout runs git pull when dir is inside a git checkout. Non-git
// local symlink installs are valid for development, so they are treated as a
// no-op rather than an update failure.
func (m *Manager) gitPullIfCheckout(dir string) error {
	cmd := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel")
	if err := cmd.Run(); err != nil {
		return nil
	}
	return m.gitPull(dir)
}

// packagesChanged compares two manifests' package lists
func (m *Manager) packagesChanged(old, new *config.Manifest) bool {
	if old.Runtime.Python != new.Runtime.Python {
		return true
	}
	if !stringSlicesEqual(old.Runtime.Packages, new.Runtime.Packages) {
		return true
	}
	if !stringSlicesEqual(old.Runtime.CondaPackages, new.Runtime.CondaPackages) {
		return true
	}
	if old.Runtime.Requirements != new.Runtime.Requirements {
		return true
	}
	return false
}

// stringSlicesEqual compares two string slices for equality
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Uninstall removes a tool. If removeEnv is true, also removes the jumpboot venv.
// The caller is responsible for stopping the tool first (via Executor.Stop).
func (m *Manager) Uninstall(name string, removeEnv bool) error {
	tool, ok := m.tools[name]
	if !ok {
		return fmt.Errorf("tool not found: %s", name)
	}

	// Remove tool directory (or symlink)
	if err := os.RemoveAll(tool.Path); err != nil {
		return fmt.Errorf("failed to remove tool directory %s: %w", tool.Path, err)
	}

	// Optionally remove the venv
	if removeEnv {
		venvPath := filepath.Join(m.cfg.EnvsDir, "venvs", name)
		os.RemoveAll(venvPath) // best-effort, may not exist
	}

	delete(m.tools, name)
	return nil
}

// Get returns a tool by name
func (m *Manager) Get(name string) (*Tool, bool) {
	t, ok := m.tools[name]
	return t, ok
}

// List returns all installed tools
func (m *Manager) List() []*Tool {
	tools := make([]*Tool, 0, len(m.tools))
	for _, t := range m.tools {
		tools = append(tools, t)
	}
	return tools
}

// ListJSON returns tools as JSON
func (m *Manager) ListJSON() ([]byte, error) {
	return json.MarshalIndent(m.List(), "", "  ")
}

// ToolInfo returns agent-friendly info about a tool
type ToolInfo struct {
	Name         string                   `json:"name"`
	Version      string                   `json:"version"`
	Description  string                   `json:"description"`
	Capabilities []string                 `json:"capabilities"`
	Mode         string                   `json:"mode"`
	Status       string                   `json:"status"`
	HealthStatus string                   `json:"health_status,omitempty"`
	Methods      map[string]config.Method `json:"methods"`
}

// Info returns detailed info about a tool
func (m *Manager) Info(name string) (*ToolInfo, error) {
	tool, ok := m.tools[name]
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", name)
	}

	return &ToolInfo{
		Name:         tool.Manifest.Name,
		Version:      tool.Manifest.Version,
		Description:  tool.Manifest.Description,
		Capabilities: tool.Manifest.Capabilities,
		Mode:         tool.Manifest.Runtime.Mode,
		Status:       tool.Status,
		HealthStatus: tool.HealthStatus,
		Methods:      tool.Manifest.RPC.Methods,
	}, nil
}

// EnsureEnvironment loads or creates the jumpboot environment for a tool
func (m *Manager) EnsureEnvironment(tool *Tool) error {
	if tool.Env != nil {
		return nil
	}

	env, err := m.createEnvironment(tool.Manifest)
	if err != nil {
		return err
	}

	if env.IsNew {
		if err := m.installPackages(env, tool.Manifest, tool.Path, false); err != nil {
			return err
		}
	}

	tool.Env = env
	return nil
}
