package tools

import (
	"os"
	"path/filepath"
	"testing"
)

// Integration tests — these actually run Python through jumpboot.
// They create real jumpboot environments and call Python code.
// Slower than unit tests but validate the full stack.

// skipIfNoJumpboot skips tests if jumpboot environments can't be created
// (e.g., mamba not available, CI without Python)
func skipIfNoJumpboot(t *testing.T) {
	t.Helper()
	if os.Getenv("JB_MESH_SKIP_INTEGRATION") == "1" {
		t.Skip("skipping integration test (JB_MESH_SKIP_INTEGRATION=1)")
	}
}

func TestExecutorCall_REPL(t *testing.T) {
	skipIfNoJumpboot(t)

	cfg := testConfig(t)
	srcDir := t.TempDir()
	writeTestTool(t, srcDir, "repl-echo", false)

	mgr := NewManager(cfg)
	mgr.LoadAll()

	tool, err := mgr.Install(filepath.Join(srcDir, "repl-echo"))
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	exec := NewExecutor(mgr)
	defer exec.Close()

	result, err := exec.Call(tool.Name, "echo", map[string]interface{}{
		"message": "hello from test",
	})
	if err != nil {
		t.Fatalf("call: %v", err)
	}

	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T: %v", result, result)
	}
	echo, ok := resultMap["echo"].(string)
	if !ok || echo != "hello from test" {
		t.Fatalf("expected 'hello from test', got %v", resultMap["echo"])
	}
}

func TestExecutorCall_Health(t *testing.T) {
	skipIfNoJumpboot(t)

	cfg := testConfig(t)
	srcDir := t.TempDir()
	writeTestTool(t, srcDir, "health-check", false)

	mgr := NewManager(cfg)
	mgr.LoadAll()

	tool, err := mgr.Install(filepath.Join(srcDir, "health-check"))
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	exec := NewExecutor(mgr)
	defer exec.Close()

	result, err := exec.Call(tool.Name, "health", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}

	resultMap := result.(map[string]interface{})
	status := resultMap["status"].(string)
	if status != "ok" {
		t.Fatalf("expected status ok, got %s", status)
	}
}

func TestExecutorCall_MessagePack(t *testing.T) {
	skipIfNoJumpboot(t)

	cfg := testConfig(t)
	toolDir := filepath.Join(t.TempDir(), "msgpack-tool")
	os.MkdirAll(toolDir, 0755)

	manifest := `name: msgpack-tool
version: 1.0.0
description: Test MessagePack transport
runtime:
  python: "3.11"
  mode: persistent
  transport: msgpack
  packages:
    - pydantic>=2.0
    - ` + testJBServicePackage(t) + `
rpc:
  methods:
    multiply:
      description: Multiply two numbers
    health:
      description: Health check
`
	os.WriteFile(filepath.Join(toolDir, "jumpboot.yaml"), []byte(manifest), 0644)

	mainPy := `from jb_service import MessagePackService, method, run

class MsgpackTool(MessagePackService):
    name = "msgpack-tool"
    version = "1.0.0"

    @method
    def multiply(self, a: float = 1, b: float = 1) -> dict:
        return {"result": a * b}

    @method
    def health(self) -> dict:
        return {"status": "ok"}

if __name__ == "__main__":
    run(MsgpackTool)
`
	os.WriteFile(filepath.Join(toolDir, "main.py"), []byte(mainPy), 0644)

	mgr := NewManager(cfg)
	mgr.LoadAll()

	tool, err := mgr.Install(toolDir)
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	exec := NewExecutor(mgr)
	defer exec.Close()

	// Start persistent tool
	if err := exec.Start(tool.Name); err != nil {
		t.Fatalf("start: %v", err)
	}

	result, err := exec.Call(tool.Name, "multiply", map[string]interface{}{
		"a": 6.0, "b": 7.0,
	})
	if err != nil {
		t.Fatalf("call: %v", err)
	}

	// Result might be unwrapped by executor — handle both cases
	switch v := result.(type) {
	case map[string]interface{}:
		val := v["result"].(float64)
		if val != 42.0 {
			t.Fatalf("expected 42, got %f", val)
		}
	case float64:
		if v != 42.0 {
			t.Fatalf("expected 42, got %f", v)
		}
	default:
		t.Fatalf("unexpected result type %T: %v", result, result)
	}
}

func TestExecutorCall_NonexistentTool(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	mgr.LoadAll()
	exec := NewExecutor(mgr)
	defer exec.Close()

	_, err := exec.Call("nonexistent", "method", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent tool")
	}
}

func TestExecutorCall_NonexistentMethod(t *testing.T) {
	skipIfNoJumpboot(t)

	cfg := testConfig(t)
	srcDir := t.TempDir()
	writeTestTool(t, srcDir, "method-test", false)

	mgr := NewManager(cfg)
	mgr.LoadAll()

	_, err := mgr.Install(filepath.Join(srcDir, "method-test"))
	if err != nil {
		t.Fatal(err)
	}

	exec := NewExecutor(mgr)
	defer exec.Close()

	_, err = exec.Call("method-test", "nonexistent", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent method")
	}
}
