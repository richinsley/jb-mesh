package node

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/richinsley/jb-mesh/pkg/mesh"
)

// TestStoppedToolThroughConfigRecoveryAndReconnect follows a persistent tool
// that is not running through a configuration change, on a node connected to
// a server that is restarted under it (a real reconnect):
//
//   - a recovery that fails leaves the tool with no route: not its mesh
//     service, not its node-targeted subjects, not its streaming subjects,
//     and a reconnect restores none of them;
//   - a recovery that succeeds leaves it with exactly one of each, before and
//     after a reconnect: one answer per request and each stream frame once;
//   - a running tool whose restart for a new configuration fails is left
//     with no route, the same way.
func TestStoppedToolThroughConfigRecoveryAndReconnect(t *testing.T) {
	if os.Getenv("JB_MESH_SKIP_INTEGRATION") == "1" {
		t.Skip("skipping integration test (JB_MESH_SKIP_INTEGRATION=1)")
	}
	sdk := jbServicePackage(t)

	// A loopback server that can be restarted on the same port.
	storeDir := t.TempDir()
	port := -1
	var server *natsserver.Server
	startServer := func() {
		t.Helper()
		ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: port, NoLog: true, NoSigs: true,
			JetStream: true, StoreDir: storeDir})
		if err != nil {
			t.Fatalf("NATS server: %v", err)
		}
		go ns.Start()
		if !ns.ReadyForConnections(5 * time.Second) {
			t.Fatal("NATS server did not start")
		}
		server, port = ns, ns.Addr().(*net.TCPAddr).Port
	}
	startServer()
	t.Cleanup(func() { server.Shutdown() })

	n, err := New(Config{HomeDir: t.TempDir(), NodeName: "rec-node", NoMDNS: true, NATSUrl: server.ClientURL(),
		DisableFileServing: true, Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(n.Close)

	toolDir := filepath.Join(t.TempDir(), "recovers")
	if err := os.MkdirAll(toolDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `name: recovers
version: 1.0.0
description: Reports its configuration and streams a few frames
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
rpc:
  methods:
    show:
      description: The configuration this process was given
    ticks:
      description: Emit one chunk per number
      stream: true
    health:
      description: Health check
`
	mainPy := `import json
import os

from jb_service import MessagePackService, method, run


class Recovers(MessagePackService):
    name = "recovers"
    version = "1.0.0"

    @method
    def show(self) -> dict:
        return {"given": True, "config": json.loads(os.environ.get("JB_TOOL_CONFIG") or "{}")}

    @method(stream=True)
    def ticks(self, upto: int = 3, ctx=None) -> dict:
        for n in range(1, int(upto) + 1):
            if ctx is not None:
                ctx.emit({"n": n})
        return {"count": int(upto), "done": True}

    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(Recovers)
`
	for name, text := range map[string]string{"jumpboot.yaml": manifest, "main.py": mainPy} {
		if err := os.WriteFile(filepath.Join(toolDir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := n.InstallTool(toolDir); err == nil {
		t.Fatal("installed and started with no configuration")
	}
	installed, ok := n.manager.Get("recovers")
	if !ok {
		t.Fatal("the tool is not installed")
	}
	entrypoint := filepath.Join(installed.Path, "main.py")

	// answersTo counts the answers one request gets on subject.
	answersTo := func(subject string) int {
		t.Helper()
		probe, err := nats.Connect(server.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		defer probe.Close()
		inbox := nats.NewInbox()
		sub, err := probe.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		if err := probe.PublishRequest(subject, inbox, []byte(`{"params":{}}`)); err != nil {
			t.Fatal(err)
		}
		count := 0
		deadline := time.Now().Add(2 * time.Second)
		for {
			msg, err := sub.NextMsg(time.Until(deadline))
			if err != nil {
				return count
			}
			if len(msg.Data) == 0 { // the server's no-responders status
				return count
			}
			count++
			if strings.Contains(string(msg.Data), `"done":true`) || !strings.Contains(subject, ".stream") {
				// one whole answer; keep listening a moment for a second copy
				deadline = time.Now().Add(500 * time.Millisecond)
			}
		}
	}
	subjects := map[string]string{
		"service":       "tools.recovers.show",
		"node-targeted": "node.rec-node.tools.recovers.show",
		"stream":        mesh.StreamSubject("recovers", "ticks"),
		"node stream":   mesh.NodeStreamSubject("rec-node", "recovers", "ticks"),
	}
	noRoutes := func(when string) {
		t.Helper()
		for name, subject := range subjects {
			if got := answersTo(subject); got != 0 {
				t.Fatalf("%s: the %s subject %s is answered (%d messages)", when, name, subject, got)
			}
		}
		if tool, _ := n.manager.Get("recovers"); tool.Status == "running" {
			t.Fatalf("%s: the tool is running", when)
		}
	}
	oneOfEach := func(when string) {
		t.Helper()
		for name, subject := range subjects {
			want := 1
			if strings.Contains(subject, ".stream") {
				want = 4 // three chunks and the terminal frame
			}
			if got := answersTo(subject); got != want {
				t.Fatalf("%s: the %s subject %s gave %d messages, want %d", when, name, subject, got, want)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		frames, err := n.Mesh().Stream(ctx, "recovers", "ticks", map[string]interface{}{"upto": 3})
		if err != nil {
			t.Fatalf("%s: stream: %v", when, err)
		}
		chunks, done := 0, false
		for f := range frames {
			if f.Error != "" {
				t.Fatalf("%s: stream frame error: %s", when, f.Error)
			}
			if f.Done {
				done = true
			} else {
				chunks++
			}
		}
		if chunks != 3 || !done {
			t.Fatalf("%s: stream gave %d chunks, done=%v", when, chunks, done)
		}
	}
	reconnect := func() {
		t.Helper()
		server.Shutdown()
		server.WaitForShutdown()
		startServer()
		deadline := time.Now().Add(20 * time.Second)
		for !n.Mesh().Conn().IsConnected() {
			if time.Now().After(deadline) {
				t.Fatal("the node did not reconnect")
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(time.Second) // let the re-registration finish
	}
	set := func(dir string) error {
		_, err := n.HandleConfig("recovers", "set", map[string]interface{}{"data_dir": dir})
		return err
	}
	waitUp := func() {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		for {
			if res, err := n.Mesh().Call("recovers", "show", nil, 5*time.Second); err == nil && res.OK {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("the tool never answered")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	noRoutes("refused at install")

	// Brought up by a valid configuration, then stopped.
	if err := set("/a"); err != nil {
		t.Fatalf("set a valid configuration: %v", err)
	}
	waitUp()
	oneOfEach("after the first recovery")
	if err := n.executor.Stop("recovers"); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// A recovery that fails: the configuration is valid, the start is not.
	hidden := entrypoint + ".hidden"
	if err := os.Rename(entrypoint, hidden); err != nil {
		t.Fatal(err)
	}
	if err := set("/b"); err == nil || !strings.Contains(err.Error(), "the tool did not start") {
		t.Fatalf("a recovery with no entrypoint: got %v", err)
	}
	noRoutes("after a failed recovery")
	reconnect()
	noRoutes("after a failed recovery and a reconnect")

	// A recovery that succeeds.
	if err := os.Rename(hidden, entrypoint); err != nil {
		t.Fatal(err)
	}
	if err := set("/c"); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	waitUp()
	oneOfEach("after recovery")
	reconnect()
	waitUp()
	oneOfEach("after recovery and a reconnect")

	// And once more through the same path, so a second recovery does not add
	// to the first.
	if err := n.executor.Stop("recovers"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := set("/d"); err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	waitUp()
	reconnect()
	waitUp()
	oneOfEach("after a second recovery and a reconnect")

	// A running tool whose restart for a new configuration fails keeps no
	// route either, and a later valid set brings it back.
	if err := os.Rename(entrypoint, hidden); err != nil {
		t.Fatal(err)
	}
	if err := set("/e"); err == nil || !strings.Contains(err.Error(), "failed to restart after config change") {
		t.Fatalf("a restart with no entrypoint: got %v", err)
	}
	noRoutes("after a failed restart of a running tool")
	reconnect()
	noRoutes("after a failed restart of a running tool and a reconnect")
	if err := os.Rename(hidden, entrypoint); err != nil {
		t.Fatal(err)
	}
	if err := set("/f"); err != nil {
		t.Fatalf("recovery after a failed restart: %v", err)
	}
	waitUp()
	oneOfEach("after recovering from a failed restart")
	reconnect()
	waitUp()
	oneOfEach("after recovering from a failed restart, and a reconnect")
}
