package tools

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/richinsley/jb-mesh/pkg/config"
)

// A tool whose configuration does not resolve is not started. Before this was
// so, the error was discarded and the tool ran with no JB_TOOL_CONFIG at all.

func configuredTool(t *testing.T, required []string) *Tool {
	t.Helper()
	tool := makeToolWithConfig(t, map[string]config.ConfigParam{
		"data_dir": {Type: "string", Default: "/declared/default"},
		"port":     {Type: "int"},
		"mode":     {Type: "string", Enum: []interface{}{"a", "b"}},
	}, required)
	tool.Manifest.Runtime.Entrypoint = "main.py"
	return tool
}

func writeConfig(t *testing.T, tool *Tool, text string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(tool.Path, "config.json"), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestToolEnv_ValidConfigIsPassedWithDefaults(t *testing.T) {
	e := &Executor{serverPort: 9800, natsURL: "nats://127.0.0.1:1"}

	tool := configuredTool(t, nil)
	env, err := e.getToolEnv(tool)
	if err != nil {
		t.Fatalf("no config.json: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal([]byte(env["JB_TOOL_CONFIG"]), &got); err != nil {
		t.Fatalf("JB_TOOL_CONFIG %q: %v", env["JB_TOOL_CONFIG"], err)
	}
	if got["data_dir"] != "/declared/default" || len(got) != 1 {
		t.Fatalf("defaults only: got %v", got)
	}
	if env["JB_NATS_URL"] != "nats://127.0.0.1:1" || env["JB_SERVE_URL"] == "" {
		t.Fatalf("the rest of the environment changed: %v", env)
	}

	writeConfig(t, tool, `{"data_dir": "/set", "port": 7, "mode": "b"}`)
	env, err = e.getToolEnv(tool)
	if err != nil {
		t.Fatalf("valid config.json: %v", err)
	}
	got = nil
	if err := json.Unmarshal([]byte(env["JB_TOOL_CONFIG"]), &got); err != nil {
		t.Fatal(err)
	}
	if got["data_dir"] != "/set" || got["port"] != float64(7) || got["mode"] != "b" {
		t.Fatalf("persisted values: got %v", got)
	}

	// A tool with no schema and no config.json is given no JB_TOOL_CONFIG, as before.
	env, err = e.getToolEnv(makeToolNoConfig(t))
	if err != nil {
		t.Fatalf("no schema: %v", err)
	}
	if _, ok := env["JB_TOOL_CONFIG"]; ok {
		t.Fatalf("no schema, no file: JB_TOOL_CONFIG should be absent, got %q", env["JB_TOOL_CONFIG"])
	}
}

func TestToolEnv_InvalidConfigIsRefused(t *testing.T) {
	e := &Executor{serverPort: 9800}
	cases := []struct {
		name, file string
		required   []string
		reason     string
	}{
		{"a key the manifest does not declare", `{"data_dri": "/typo"}`, nil, `unknown field "data_dri"`},
		{"a value of the wrong type", `{"port": "7"}`, nil, `field "port" expected int`},
		{"a value outside its enum", `{"mode": "c"}`, nil, `not in allowed values`},
		{"a required field with no value", `{"mode": "a"}`, []string{"port"}, `required field "port" missing`},
		{"a required field and no file at all", "", []string{"port"}, `required field "port" missing`},
		{"a file that is not JSON", `{"port": `, nil, "parse config"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tool := configuredTool(t, c.required)
			if c.file != "" {
				writeConfig(t, tool, c.file)
			}
			env, err := e.getToolEnv(tool)
			if err == nil {
				t.Fatalf("resolved to %v", env)
			}
			if env != nil {
				t.Fatalf("an environment was returned with the error: %v", env)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error is not ErrInvalidConfig: %v", err)
			}
			for _, want := range []string{"tool test-tool not started", c.reason} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not say %q", err, want)
				}
			}
		})
	}
}

// Each of the four ways a tool process is made asks for the configuration
// before anything else. The tool here has no environment and no entrypoint
// file: reaching process creation or the entrypoint would fail differently
// (or panic), so getting ErrInvalidConfig back shows nothing was started.
func TestNoProcessIsMadeForAnInvalidConfig(t *testing.T) {
	e := NewExecutor(NewManager(testConfig(t)))
	defer e.Close()
	tool := configuredTool(t, nil)
	writeConfig(t, tool, `{"not_declared": true}`)

	starts := map[string]func() error{
		"one-shot, REPL":      func() error { _, err := e.callOneshot(tool, "health", nil); return err },
		"one-shot, msgpack":   func() error { _, err := e.callOneshotMsgpack(tool, "health", nil); return err },
		"persistent, REPL":    func() error { return e.startRepl(tool) },
		"persistent, msgpack": func() error { return e.startMsgpack(tool) },
	}
	for name, start := range starts {
		if err := start(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: want ErrInvalidConfig, got %v", name, err)
		}
	}
	if tool.Status == "running" || len(e.repls) != 0 || len(e.queues) != 0 {
		t.Fatalf("something was started: status %q, %d REPLs, %d queues", tool.Status, len(e.repls), len(e.queues))
	}
}
