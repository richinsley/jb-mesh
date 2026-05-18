package mesh

import (
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// startTestNATS spins up an in-process NATS server on a random port for testing
func startTestNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1, // random port
		NoLog:  true,
		NoSigs: true,
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server failed to start")
	}
	t.Cleanup(func() { ns.Shutdown() })
	return ns
}

func TestMeshConnect(t *testing.T) {
	ns := startTestNATS(t)
	m, err := New(Config{
		NATSUrl:  ns.ClientURL(),
		NodeName: "test-node",
	})
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer m.Close()

	if !m.Connected() {
		t.Fatal("expected connected")
	}
}

func TestMeshConnectBadURL(t *testing.T) {
	_, err := New(Config{
		NATSUrl:  "nats://127.0.0.1:1",
		NodeName: "test-node",
	})
	if err == nil {
		t.Fatal("expected error connecting to bad URL")
	}
}

func TestRegisterAndCallTool(t *testing.T) {
	ns := startTestNATS(t)

	// Node A registers a tool
	nodeA, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "node-a"})
	if err != nil {
		t.Fatalf("node-a connect: %v", err)
	}
	defer nodeA.Close()

	err = nodeA.RegisterTool("adder", "1.0.0", "adds numbers", []string{"add"},
		func(req CallRequest, method string, params map[string]interface{}) (interface{}, error) {
			a := params["a"].(float64)
			b := params["b"].(float64)
			return map[string]interface{}{"sum": a + b}, nil
		},
	)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Node B calls the tool
	nodeB, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "node-b"})
	if err != nil {
		t.Fatalf("node-b connect: %v", err)
	}
	defer nodeB.Close()

	result, err := nodeB.Call("adder", "add", map[string]interface{}{"a": 3.0, "b": 4.0}, 5*time.Second)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !result.OK {
		t.Fatalf("expected OK, got error: %s", result.Error)
	}

	resultMap, ok := result.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", result.Result)
	}
	sum, ok := resultMap["sum"].(float64)
	if !ok || sum != 7.0 {
		t.Fatalf("expected sum 7, got %v", resultMap["sum"])
	}
}

func TestCallNonexistentTool(t *testing.T) {
	ns := startTestNATS(t)
	m, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	_, err = m.Call("nonexistent", "method", nil, 1*time.Second)
	if err == nil {
		t.Fatal("expected error calling nonexistent tool")
	}
}

func TestServiceDiscovery(t *testing.T) {
	ns := startTestNATS(t)

	node, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "discovery-node"})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	err = node.RegisterTool("finder", "2.0.0", "a test tool", []string{"search", "index"},
		func(req CallRequest, method string, params map[string]interface{}) (interface{}, error) {
			return "ok", nil
		},
	)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Give NATS a moment to propagate
	time.Sleep(100 * time.Millisecond)

	services, err := node.ListServices()
	if err != nil {
		t.Fatalf("list services: %v", err)
	}

	found := false
	for _, svc := range services {
		if svc.Name == "finder" {
			found = true
			if svc.Version != "2.0.0" {
				t.Fatalf("expected version 2.0.0, got %s", svc.Version)
			}
		}
	}
	if !found {
		t.Fatal("finder service not discovered")
	}
}

func TestInstallRequestReply(t *testing.T) {
	ns := startTestNATS(t)

	// Serving node subscribes to install requests
	server, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "server"})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	err = server.SubscribeInstall(func(source string) (string, string, error) {
		// Simulate successful install
		return "test-tool", "1.0.0", nil
	})
	if err != nil {
		t.Fatalf("subscribe install: %v", err)
	}

	// Client requests install
	client, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "client"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	result, err := client.RequestInstall("server", "http://example.com/tool.git", 5*time.Second)
	if err != nil {
		t.Fatalf("request install: %v", err)
	}
	if !result.OK {
		t.Fatalf("expected OK, got error: %s", result.Error)
	}
	if result.ToolName != "test-tool" {
		t.Fatalf("expected test-tool, got %s", result.ToolName)
	}
	if result.Version != "1.0.0" {
		t.Fatalf("expected 1.0.0, got %s", result.Version)
	}
}

func TestInstallRequestFails(t *testing.T) {
	ns := startTestNATS(t)

	server, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "server"})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	err = server.SubscribeInstall(func(source string) (string, string, error) {
		return "", "", fmt.Errorf("clone failed: repo not found")
	})
	if err != nil {
		t.Fatal(err)
	}

	client, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "client"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	result, err := client.RequestInstall("server", "http://bad-url.git", 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if result.OK {
		t.Fatal("expected failure result")
	}
	if result.Error == "" {
		t.Fatal("expected error message")
	}
}

func TestCallPropagatesCorr(t *testing.T) {
	ns := startTestNATS(t)
	server, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "server"})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	corrCh := make(chan string, 1)
	err = server.RegisterTool("corr-tool", "1.0.0", "corr test", []string{"ping"}, func(req CallRequest, method string, params map[string]interface{}) (interface{}, error) {
		corrCh <- req.Corr
		return "ok", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	client, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "client"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Call("corr-tool", "ping", map[string]interface{}{"corr": "corr-xyz"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-corrCh:
		if got != "corr-xyz" {
			t.Fatalf("corr = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for corr")
	}
}

func TestMultiNodeLoadBalancing(t *testing.T) {
	ns := startTestNATS(t)

	// Two nodes register the same tool
	counts := map[string]int{"node-a": 0, "node-b": 0}

	for _, name := range []string{"node-a", "node-b"} {
		nodeName := name
		node, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: nodeName})
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()

		err = node.RegisterTool("shared-tool", "1.0.0", "shared", []string{"ping"},
			func(req CallRequest, method string, params map[string]interface{}) (interface{}, error) {
				return map[string]interface{}{"node": nodeName}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Client calls the tool multiple times
	client, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "client"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for i := 0; i < 20; i++ {
		result, err := client.Call("shared-tool", "ping", nil, 5*time.Second)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		resultMap := result.Result.(map[string]interface{})
		node := resultMap["node"].(string)
		counts[node]++
	}

	// Both nodes should have received calls (NATS queue group load balancing)
	if counts["node-a"] == 0 || counts["node-b"] == 0 {
		t.Fatalf("expected both nodes to receive calls, got: %v", counts)
	}
}
