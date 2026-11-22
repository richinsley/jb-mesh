package node

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/richinsley/jb-mesh/pkg/tools"
)

// TestToolWithInvalidConfigIsNotStarted runs a real persistent Python tool
// behind a real node. The tool needs one configuration value and reports the
// configuration it was started with.
//
//   - Installed with no value for the required field, it is not started and
//     does not answer on the mesh. It used to start with no configuration.
//   - Setting a configuration the manifest does not allow is refused and
//     changes nothing.
//   - Setting a valid one starts it, and it reports that configuration,
//     declared default included.
//   - With config.json then made invalid behind the node's back, a restart is
//     refused and the tool is left stopped; a valid set brings it back.
func TestToolWithInvalidConfigIsNotStarted(t *testing.T) {
	if os.Getenv("JB_MESH_SKIP_INTEGRATION") == "1" {
		t.Skip("skipping integration test (JB_MESH_SKIP_INTEGRATION=1)")
	}
	sdk := jbServicePackage(t)
	n, err := New(Config{HomeDir: t.TempDir(), NodeName: "cfg-node", Role: "seed", NoMDNS: true,
		EmbedHost: "127.0.0.1", EmbedPort: -1, LeafHost: "127.0.0.1", LeafPort: -1,
		DisableFileServing: true, Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(n.Close)

	toolDir := filepath.Join(t.TempDir(), "needs-config")
	if err := os.MkdirAll(toolDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `name: needs-config
version: 1.0.0
description: Reports the configuration it was started with
runtime:
  python: "3.11"
  mode: persistent
  transport: msgpack
  packages:
    - pydantic>=2.0
    - ` + sdk + `
config:
  required: [data_dir]
  schema:
    data_dir:
      type: string
    batch:
      type: int
      default: 8
rpc:
  methods:
    show:
      description: The configuration this process was given
    health:
      description: Health check
`
	mainPy := `import json
import os

from jb_service import MessagePackService, method, run


class NeedsConfig(MessagePackService):
    name = "needs-config"
    version = "1.0.0"

    @method
    def show(self) -> dict:
        raw = os.environ.get("JB_TOOL_CONFIG")
        return {"given": raw is not None, "config": json.loads(raw or "{}")}

    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(NeedsConfig)
`
	for name, text := range map[string]string{"jumpboot.yaml": manifest, "main.py": mainPy} {
		if err := os.WriteFile(filepath.Join(toolDir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	answers := func() bool {
		res, err := n.Mesh().Call("needs-config", "show", nil, 3*time.Second)
		return err == nil && res.OK
	}
	status := func() string {
		tool, ok := n.manager.Get("needs-config")
		if !ok {
			t.Fatal("the tool is not installed")
		}
		return tool.Status
	}

	// Installed, but with nothing for the required field.
	_, _, err = n.InstallTool(toolDir)
	if !errors.Is(err, tools.ErrInvalidConfig) || !strings.Contains(err.Error(), `required field "data_dir" missing`) {
		t.Fatalf("install with no configuration: want a refused start naming the field, got %v", err)
	}
	if status() == "running" || answers() {
		t.Fatalf("the tool is up without its configuration (status %q)", status())
	}

	// A configuration the manifest does not allow is not saved.
	if _, err := n.HandleConfig("needs-config", "set", map[string]interface{}{"data_dir": "/d", "data_dri": "/typo"}); err == nil ||
		!strings.Contains(err.Error(), `unknown field "data_dri"`) {
		t.Fatalf("set with an undeclared key: got %v", err)
	}
	installed, _ := n.manager.Get("needs-config")
	configFile := filepath.Join(installed.Path, "config.json")
	if _, err := os.Stat(configFile); !os.IsNotExist(err) {
		t.Fatalf("a refused set left a config.json (stat: %v)", err)
	}
	if status() == "running" {
		t.Fatal("a refused set started the tool")
	}

	// A valid one starts it.
	if _, err := n.HandleConfig("needs-config", "set", map[string]interface{}{"data_dir": "/d"}); err != nil {
		t.Fatalf("set a valid configuration: %v", err)
	}
	show := func() map[string]interface{} {
		t.Helper()
		res, err := n.Mesh().Call("needs-config", "show", nil, 30*time.Second)
		if err != nil || !res.OK {
			t.Fatalf("show: %v %+v", err, res)
		}
		out, _ := res.Result.(map[string]interface{})
		cfg, _ := out["config"].(map[string]interface{})
		if out["given"] != true || cfg == nil {
			t.Fatalf("show returned %v", res.Result)
		}
		return cfg
	}
	if cfg := show(); cfg["data_dir"] != "/d" || cfg["batch"] != float64(8) || len(cfg) != 2 {
		t.Fatalf("the tool was started with %v", cfg)
	}

	// A change to a running tool restarts it, as before.
	if _, err := n.HandleConfig("needs-config", "set", map[string]interface{}{"data_dir": "/e", "batch": 2}); err != nil {
		t.Fatalf("change the configuration: %v", err)
	}
	if cfg := show(); cfg["data_dir"] != "/e" || cfg["batch"] != float64(2) {
		t.Fatalf("after a change the tool has %v", cfg)
	}

	// config.json edited by hand into something the manifest does not allow.
	if err := os.WriteFile(configFile, []byte(`{"data_dir": "/e", "extra": 1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := n.executor.Stop("needs-config"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := n.executor.Start("needs-config"); !errors.Is(err, tools.ErrInvalidConfig) || !strings.Contains(err.Error(), `unknown field "extra"`) {
		t.Fatalf("start on an invalid config.json: got %v", err)
	}
	if status() == "running" {
		t.Fatal("the tool is running on an invalid config.json")
	}
	if _, err := n.HandleConfig("needs-config", "get", nil); err == nil {
		t.Fatal("reading an invalid configuration reported no error")
	}

	// Setting a valid configuration replaces the file and brings the tool back.
	if _, err := n.HandleConfig("needs-config", "set", map[string]interface{}{"data_dir": "/f"}); err != nil {
		t.Fatalf("set after a refused start: %v", err)
	}
	if cfg := show(); cfg["data_dir"] != "/f" || cfg["batch"] != float64(8) {
		t.Fatalf("after recovery the tool has %v", cfg)
	}
}
