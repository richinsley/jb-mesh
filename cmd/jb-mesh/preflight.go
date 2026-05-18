package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/richinsley/jb-mesh/pkg/config"
	"github.com/richinsley/jb-mesh/pkg/sdk"
	toolstate "github.com/richinsley/jb-mesh/pkg/tools"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const preflightTimeout = 10 * time.Minute

type preflightCheck struct {
	Name    string
	Status  string
	Message string
}

type pythonProbe struct {
	Methods          []string `json:"methods"`
	FunctionNames    []string `json:"function_names"`
	TopLevelJumpboot bool     `json:"top_level_jumpboot"`
}

type importProbe struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func preflightCmd() *cobra.Command {
	var pythonBin string
	var keepVenv bool
	var skipInstall bool

	cmd := &cobra.Command{
		Use:   "preflight <tool-path>",
		Short: "Validate a jb-mesh Python tool before deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			toolPath, err := filepath.Abs(args[0])
			if err != nil {
				return err
			}

			result, err := runPreflight(toolPath, pythonBin, keepVenv, skipInstall)
			if err != nil {
				return err
			}

			hasFailure := false
			for _, check := range result {
				fmt.Printf("[%s] %s: %s\n", check.Status, check.Name, check.Message)
				if check.Status == "FAIL" {
					hasFailure = true
				}
			}
			if hasFailure {
				return fmt.Errorf("preflight failed")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&pythonBin, "python", "python3", "Python interpreter to use for venv/import checks")
	cmd.Flags().BoolVar(&keepVenv, "keep-venv", false, "Keep the temporary preflight virtualenv instead of deleting it")
	cmd.Flags().BoolVar(&skipInstall, "skip-install", false, "Skip virtualenv package installation and import smoke test")
	return cmd
}

func runPreflight(toolPath, pythonBin string, keepVenv, skipInstall bool) ([]preflightCheck, error) {
	checks := make([]preflightCheck, 0, 12)

	manifest, err := loadManifest(toolPath)
	if err != nil {
		checks = append(checks, preflightCheck{Name: "manifest", Status: "FAIL", Message: err.Error()})
		return checks, nil
	}
	checks = append(checks, preflightCheck{Name: "manifest", Status: "PASS", Message: fmt.Sprintf("loaded %s v%s", manifest.Name, manifest.Version)})

	entrypoint := filepath.Join(toolPath, manifest.Runtime.Entrypoint)
	if _, err := os.Stat(entrypoint); err != nil {
		checks = append(checks, preflightCheck{Name: "entrypoint", Status: "FAIL", Message: fmt.Sprintf("missing %s", manifest.Runtime.Entrypoint)})
		return checks, nil
	}
	checks = append(checks, preflightCheck{Name: "entrypoint", Status: "PASS", Message: manifest.Runtime.Entrypoint})

	probe, err := inspectPythonEntrypoint(pythonBin, entrypoint)
	if err != nil {
		checks = append(checks, preflightCheck{Name: "python-ast", Status: "FAIL", Message: err.Error()})
		return checks, nil
	}
	checks = append(checks, preflightCheck{Name: "python-ast", Status: "PASS", Message: fmt.Sprintf("found %d @method definitions", len(probe.Methods))})
	if probe.TopLevelJumpboot {
		checks = append(checks, preflightCheck{Name: "jumpboot-import", Status: "WARN", Message: "top-level jumpboot import detected; defer it to runtime/bootstrap"})
	}

	checks = append(checks, compareMethodSets(manifest, probe)...)
	checks = append(checks, validateManifestHooks(manifest, probe)...)
	checks = append(checks, validateConfigSchema(manifest)...)

	if skipInstall {
		checks = append(checks, preflightCheck{Name: "venv", Status: "WARN", Message: "skipped package install/import smoke (--skip-install)"})
		return checks, nil
	}

	venvDir, installCheck, err := createAndInstallVenv(toolPath, pythonBin, manifest)
	checks = append(checks, installCheck)
	if err != nil {
		return checks, nil
	}
	if !keepVenv {
		defer os.RemoveAll(venvDir)
	} else {
		checks = append(checks, preflightCheck{Name: "venv-path", Status: "PASS", Message: venvDir})
	}

	pythonExec := filepath.Join(venvDir, "bin", "python")
	importCheck, err := runImportSmoke(toolPath, entrypoint, pythonExec)
	checks = append(checks, importCheck)
	if err != nil {
		return checks, nil
	}

	return checks, nil
}

func loadManifest(toolPath string) (*config.Manifest, error) {
	toolPath, err := toolstate.ResolveToolSource(toolPath)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(toolPath, "jumpboot.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var manifest config.Manifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	manifest.ApplyDefaults()
	if strings.TrimSpace(manifest.Name) == "" {
		return nil, fmt.Errorf("manifest: name is required")
	}
	if strings.TrimSpace(manifest.Version) == "" {
		return nil, fmt.Errorf("manifest %s: version is required", manifest.Name)
	}
	return &manifest, nil
}

func inspectPythonEntrypoint(pythonBin, entrypoint string) (*pythonProbe, error) {
	script := strings.TrimSpace(`
import ast
import json
import sys

path = sys.argv[1]
with open(path, 'r', encoding='utf-8') as f:
    tree = ast.parse(f.read(), filename=path)

methods = []
function_names = []
top_level_jumpboot = False

for node in tree.body:
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.name.split('.')[0] == 'jumpboot':
                top_level_jumpboot = True
    elif isinstance(node, ast.ImportFrom):
        if (node.module or '').split('.')[0] == 'jumpboot':
            top_level_jumpboot = True

for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        function_names.append(node.name)
        for dec in node.decorator_list:
            name = None
            if isinstance(dec, ast.Name):
                name = dec.id
            elif isinstance(dec, ast.Attribute):
                name = dec.attr
            elif isinstance(dec, ast.Call):
                target = dec.func
                if isinstance(target, ast.Name):
                    name = target.id
                elif isinstance(target, ast.Attribute):
                    name = target.attr
            if name == 'method':
                methods.append(node.name)
                break

print(json.dumps({
    'methods': sorted(set(methods)),
    'function_names': sorted(set(function_names)),
    'top_level_jumpboot': top_level_jumpboot,
}))
`)

	stdout, err := runCommand(preflightTimeout, "", nil, pythonBin, "-c", script, entrypoint)
	if err != nil {
		return nil, fmt.Errorf("python AST probe failed: %w", err)
	}
	var probe pythonProbe
	if err := json.Unmarshal(stdout, &probe); err != nil {
		return nil, fmt.Errorf("decode python AST probe: %w", err)
	}
	return &probe, nil
}

func compareMethodSets(manifest *config.Manifest, probe *pythonProbe) []preflightCheck {
	manifestMethods := sortedKeys(manifest.RPC.Methods)
	pythonMethods := append([]string(nil), probe.Methods...)
	sort.Strings(pythonMethods)

	missingInCode := diffStrings(manifestMethods, pythonMethods)
	missingInManifest := diffStrings(pythonMethods, manifestMethods)

	checks := make([]preflightCheck, 0, 2)
	if len(missingInCode) == 0 && len(missingInManifest) == 0 {
		checks = append(checks, preflightCheck{
			Name:    "method-parity",
			Status:  "PASS",
			Message: fmt.Sprintf("manifest and code agree on %d RPC methods", len(manifestMethods)),
		})
		return checks
	}
	if len(missingInCode) > 0 {
		checks = append(checks, preflightCheck{
			Name:    "method-parity",
			Status:  "FAIL",
			Message: fmt.Sprintf("manifest methods missing in code: %s", strings.Join(missingInCode, ", ")),
		})
	}
	if len(missingInManifest) > 0 {
		checks = append(checks, preflightCheck{
			Name:    "method-parity",
			Status:  "FAIL",
			Message: fmt.Sprintf("@method functions missing in manifest: %s", strings.Join(missingInManifest, ", ")),
		})
	}
	return checks
}

func validateManifestHooks(manifest *config.Manifest, probe *pythonProbe) []preflightCheck {
	checks := make([]preflightCheck, 0, 2)
	functions := make(map[string]struct{}, len(probe.FunctionNames))
	for _, name := range probe.FunctionNames {
		functions[name] = struct{}{}
	}

	if manifest.Health != nil && manifest.Health.Method != "" {
		if _, ok := functions[manifest.Health.Method]; ok {
			checks = append(checks, preflightCheck{Name: "health-hook", Status: "PASS", Message: fmt.Sprintf("health method %q exists", manifest.Health.Method)})
		} else {
			checks = append(checks, preflightCheck{Name: "health-hook", Status: "FAIL", Message: fmt.Sprintf("health method %q not found in code", manifest.Health.Method)})
		}
	}
	if manifest.Setup != nil && manifest.Setup.Method != "" {
		if _, ok := functions[manifest.Setup.Method]; ok {
			checks = append(checks, preflightCheck{Name: "setup-hook", Status: "PASS", Message: fmt.Sprintf("setup method %q exists", manifest.Setup.Method)})
		} else {
			checks = append(checks, preflightCheck{Name: "setup-hook", Status: "FAIL", Message: fmt.Sprintf("setup method %q not found in code", manifest.Setup.Method)})
		}
	}
	return checks
}

func validateConfigSchema(manifest *config.Manifest) []preflightCheck {
	if manifest.Config == nil || len(manifest.Config.Required) == 0 {
		return []preflightCheck{{Name: "config-schema", Status: "PASS", Message: "no required config keys declared"}}
	}
	missing := make([]string, 0)
	for _, key := range manifest.Config.Required {
		if _, ok := manifest.Config.Schema[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return []preflightCheck{{Name: "config-schema", Status: "FAIL", Message: fmt.Sprintf("required config keys missing from schema: %s", strings.Join(missing, ", "))}}
	}
	return []preflightCheck{{Name: "config-schema", Status: "PASS", Message: fmt.Sprintf("%d required config keys defined in schema", len(manifest.Config.Required))}}
}

func createAndInstallVenv(toolPath, pythonBin string, manifest *config.Manifest) (string, preflightCheck, error) {
	venvDir, err := os.MkdirTemp("", "jb-mesh-preflight-venv-")
	if err != nil {
		return "", preflightCheck{Name: "venv", Status: "FAIL", Message: err.Error()}, err
	}

	if _, err := runCommand(preflightTimeout, toolPath, nil, pythonBin, "-m", "venv", venvDir); err != nil {
		return venvDir, preflightCheck{Name: "venv", Status: "FAIL", Message: fmt.Sprintf("create venv: %v", err)}, err
	}

	pipBin := filepath.Join(venvDir, "bin", "pip")
	args := []string{"install", "--disable-pip-version-check"}
	if manifest.Runtime.Requirements != "" {
		args = append(args, "-r", filepath.Join(toolPath, manifest.Runtime.Requirements))
	}
	packages := sdk.RewritePythonSDKPackages(manifest.Runtime.Packages)
	args = append(args, packages...)
	if len(args) == 2 {
		return venvDir, preflightCheck{Name: "venv", Status: "PASS", Message: "created temp venv (no runtime packages declared)"}, nil
	}

	env := map[string]string{"PIP_DISABLE_PIP_VERSION_CHECK": "1"}
	if _, err := runCommand(preflightTimeout, toolPath, env, pipBin, args...); err != nil {
		return venvDir, preflightCheck{Name: "venv", Status: "FAIL", Message: fmt.Sprintf("install runtime packages: %v", err)}, err
	}
	return venvDir, preflightCheck{Name: "venv", Status: "PASS", Message: fmt.Sprintf("created temp venv and installed %d package entries", len(packages))}, nil
}

func runImportSmoke(toolPath, entrypoint, pythonExec string) (preflightCheck, error) {
	script := strings.TrimSpace(`
import importlib.util
import json
import os
import sys
import traceback

entrypoint = sys.argv[1]
os.environ.setdefault('JB_TOOL_CONFIG', '{}')
spec = importlib.util.spec_from_file_location('_jbmesh_preflight_entrypoint', entrypoint)
module = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(module)
    print(json.dumps({'ok': True}))
except Exception as exc:
    print(json.dumps({'ok': False, 'error': ''.join(traceback.format_exception_only(type(exc), exc)).strip()}))
    sys.exit(1)
`)

	stdout, err := runCommand(preflightTimeout, toolPath, map[string]string{"JB_TOOL_CONFIG": "{}"}, pythonExec, "-c", script, entrypoint)
	var probe importProbe
	if len(stdout) > 0 {
		_ = json.Unmarshal(stdout, &probe)
	}
	if err != nil {
		message := probe.Error
		if message == "" {
			message = err.Error()
		}
		return preflightCheck{Name: "import-smoke", Status: "FAIL", Message: message}, err
	}
	return preflightCheck{Name: "import-smoke", Status: "PASS", Message: "entrypoint imported successfully in clean venv"}, nil
}

func runCommand(timeout time.Duration, workdir string, extraEnv map[string]string, bin string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = workdir
	cmd.Env = os.Environ()
	for k, v := range extraEnv {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return stdout.Bytes(), fmt.Errorf("timed out after %s", timeout)
	}
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message == "" {
			message = err.Error()
		}
		return stdout.Bytes(), fmt.Errorf("%s", message)
	}
	return stdout.Bytes(), nil
}

func sortedKeys(m map[string]config.Method) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func diffStrings(a, b []string) []string {
	bSet := make(map[string]struct{}, len(b))
	for _, value := range b {
		bSet[value] = struct{}{}
	}
	missing := make([]string, 0)
	for _, value := range a {
		if _, ok := bSet[value]; !ok {
			missing = append(missing, value)
		}
	}
	return missing
}
