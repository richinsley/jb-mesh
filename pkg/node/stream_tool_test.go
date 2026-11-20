package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	"github.com/richinsley/jb-mesh/pkg/replytoken"
)

// jbServicePackage locates the SDK source beside this repository, as the
// tools tests do, so the tool's environment installs the SDK under test.
func jbServicePackage(t *testing.T) string {
	t.Helper()
	if pkg := os.Getenv("JB_MESH_TEST_JB_SERVICE_PACKAGE"); pkg != "" {
		return pkg
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, "sdk", "python", "jb-service")
		if _, err := os.Stat(filepath.Join(candidate, "pyproject.toml")); err == nil {
			return candidate
		}
	}
	t.Skip("the jb-service SDK source was not found beside this repository")
	return ""
}

// TestToolBackedStreamOnATypedSeed runs a real Python tool behind a real node
// that joined a seed with typed authorization, and checks two things the
// reply-token design leans on:
//
//   - every frame of a stream the node pumps from the tool process carries the
//     request's token, so a caller that requires it receives the whole stream
//     and nothing injected into it;
//   - the tool process itself, with what the node gives it, cannot log in to
//     the seed. It is handed the seed's address and no credential.
func TestToolBackedStreamOnATypedSeed(t *testing.T) {
	if os.Getenv("JB_MESH_SKIP_INTEGRATION") == "1" {
		t.Skip("skipping integration test (JB_MESH_SKIP_INTEGRATION=1)")
	}
	sdk := jbServicePackage(t)
	// Broad grants: this test is about the token and the tool's login, not
	// about subject permissions.
	grant := []string{"node.>", "tools.>", "$SRV.>", "_INBOX.>", "events.>", "logs.>", "files.>", "cancel.>", "$JS.>", "$O.>", "$KV.>", "logstore.>"}
	principal := func(name string) AuthorizedPrincipal {
		return AuthorizedPrincipal{PrincipalID: name, Password: name + "-secret", PublishAllow: grant, SubscribeAllow: grant}
	}
	seed, err := New(Config{HomeDir: t.TempDir(), NodeName: "typed-seed", Role: "seed", NoMDNS: true,
		EmbedHost: "127.0.0.1", EmbedPort: -1, LeafHost: "127.0.0.1", LeafPort: -1,
		Authorization: Authorization{Enabled: true, Principals: []AuthorizedPrincipal{principal("leaf-a"), principal("op"), principal("mallory")}},
		Logging:       LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatalf("start typed seed: %v", err)
	}
	t.Cleanup(seed.Close)
	url := seed.Mesh().Conn().ConnectedUrl()

	leaf, err := New(Config{HomeDir: t.TempDir(), NodeName: "leaf-a", NoMDNS: true, NATSUrl: url,
		Username: "leaf-a", Password: "leaf-a-secret", DisableFileServing: true, Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatalf("leaf could not join: %v", err)
	}
	t.Cleanup(leaf.Close)

	toolDir := filepath.Join(t.TempDir(), "counter")
	if err := os.MkdirAll(toolDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `name: counter
version: 1.0.0
description: A streaming test tool
runtime:
  python: "3.11"
  mode: persistent
  transport: msgpack
  packages:
    - pydantic>=2.0
    - ` + sdk + `
rpc:
  methods:
    count:
      description: Emit one chunk per number, then return the sum
      stream: true
    bus_probe:
      description: Try to reach the bus with what the node gave this process
    health:
      description: Health check
`
	mainPy := `import asyncio
import os
import threading
import time

from jb_service import MessagePackService, method, run


class Counter(MessagePackService):
    name = "counter"
    version = "1.0.0"

    @method(stream=True)
    def count(self, upto: int = 5, pause_ms: int = 100, ctx=None) -> dict:
        total = 0
        for n in range(1, int(upto) + 1):
            total += n
            if ctx is not None:
                ctx.emit({"n": n})
            time.sleep(int(pause_ms) / 1000.0)
        return {"sum": total}

    @method
    def bus_probe(self) -> dict:
        url = os.environ.get("JB_NATS_URL", "")
        secretish = sorted(k for k in os.environ if "NATS" in k.upper() and any(
            w in k.upper() for w in ("USER", "PASS", "TOKEN", "CRED", "NKEY")))
        out = {"url_given": bool(url), "url_has_userinfo": "@" in url, "credential_env": secretish, "connected": False, "connect_error": ""}
        def attempt():
            # A thread of its own: the service already runs an event loop.
            try:
                import nats

                async def go():
                    nc = await asyncio.wait_for(
                        nats.connect(url, connect_timeout=2, max_reconnect_attempts=0, allow_reconnect=False), timeout=4)
                    await nc.close()

                asyncio.run(go())
                out["connected"] = True
            except Exception as e:
                out["connect_error"] = type(e).__name__ + ": " + str(e)[:200]

        worker = threading.Thread(target=attempt)
        worker.start()
        worker.join(10)
        return out

    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(Counter)
`
	if err := os.WriteFile(filepath.Join(toolDir, "jumpboot.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(toolDir, "main.py"), []byte(mainPy), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := leaf.InstallTool(toolDir); err != nil {
		t.Fatalf("install the streaming tool on the leaf: %v", err)
	}

	client, err := mesh.New(mesh.Config{NATSUrl: url, NodeName: "op", Username: "op", Password: "op-secret",
		RequireReplyToken: true, Logging: mesh.LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// Wait until the tool answers; installing builds its environment.
	deadline := time.Now().Add(4 * time.Minute)
	for {
		if res, err := client.Call("counter", "health", nil, 5*time.Second, "leaf-a"); err == nil && res.OK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the tool never became healthy on the leaf")
		}
		time.Sleep(500 * time.Millisecond)
	}

	// An adversary admitted to the seed, handed the stream's reply subject.
	mallory, err := nats.Connect(url, nats.UserInfo("mallory", "mallory-secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer mallory.Close()
	subject := mesh.NodeStreamSubject("leaf-a", "counter", "count")
	replies := make(chan string, 1)
	if _, err := mallory.Subscribe(subject, func(m *nats.Msg) {
		select {
		case replies <- m.Reply:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := mallory.Flush(); err != nil {
		t.Fatal(err)
	}

	t.Run("every frame the node pumps from the tool carries the token", func(t *testing.T) {
		before := client.ReplyRejections()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		frames, err := client.Stream(ctx, "counter", "count", map[string]interface{}{"upto": 6, "pause_ms": 120}, "leaf-a")
		if err != nil {
			t.Fatal(err)
		}
		reply := <-replies
		// Watch the wire, and inject into the middle of the real stream.
		var wire sync.Mutex
		var tokens []string
		if _, err := mallory.Subscribe(reply, func(m *nats.Msg) {
			wire.Lock()
			tokens = append(tokens, replytoken.Of(m))
			wire.Unlock()
		}); err != nil {
			t.Fatal(err)
		}
		_ = mallory.Flush()
		go func() {
			time.Sleep(250 * time.Millisecond)
			for _, f := range []mesh.StreamFrame{{Chunk: map[string]interface{}{"n": "FORGED"}}, {Result: "FORGED END", Done: true}} {
				data, _ := json.Marshal(f)
				_ = mallory.Publish(reply, data)
			}
			_ = mallory.Flush()
		}()
		var got []mesh.StreamFrame
		for f := range frames {
			got = append(got, f)
		}
		if len(got) != 7 {
			t.Fatalf("the caller received %d frames, want 6 chunks and the terminal: %+v", len(got), got)
		}
		for i := 0; i < 6; i++ {
			chunk, _ := got[i].Chunk.(map[string]interface{})
			if got[i].Done || got[i].Error != "" || chunk["n"] != float64(i+1) {
				t.Fatalf("frame %d = %+v", i, got[i])
			}
		}
		last := got[6]
		result, _ := last.Result.(map[string]interface{})
		if !last.Done || last.Error != "" || result["sum"] != float64(21) {
			t.Fatalf("terminal = %+v", last)
		}
		if n := client.ReplyRejections() - before; n != 2 {
			t.Fatalf("rejections = %d, want the 2 injected frames", n)
		}
		// On the wire: the node's frames each carried a token, all the same
		// one; the injected ones carried none.
		time.Sleep(200 * time.Millisecond)
		wire.Lock()
		defer wire.Unlock()
		with, without := 0, 0
		first := ""
		for _, tok := range tokens {
			if tok == "" {
				without++
				continue
			}
			if first == "" {
				first = tok
			}
			if tok != first || len(tok) != 2*replytoken.Size {
				t.Fatalf("the node's frames carried different or malformed tokens: %q", tokens)
			}
			with++
		}
		if without != 2 || with < 6 {
			t.Fatalf("on the wire: %d frames with the token, %d without; want at least 6 and exactly 2", with, without)
		}
	})

	t.Run("the tool process cannot log in to the seed with what it is given", func(t *testing.T) {
		res, err := client.Call("counter", "bus_probe", nil, 30*time.Second, "leaf-a")
		if err != nil || !res.OK {
			t.Fatalf("bus_probe: %+v %v", res, err)
		}
		probe, _ := res.Result.(map[string]interface{})
		t.Logf("the tool process reported: %v", probe)
		if probe["url_given"] != true {
			t.Fatalf("the tool was not given the bus address; the probe proves nothing: %v", probe)
		}
		if probe["url_has_userinfo"] != false {
			t.Fatalf("the address handed to the tool carries login details: %v", probe)
		}
		if env, _ := probe["credential_env"].([]interface{}); len(env) != 0 {
			t.Fatalf("the tool's environment holds credential-shaped variables: %v", env)
		}
		if probe["connected"] != false {
			t.Fatalf("the tool process logged in to a typed seed: %v", probe)
		}
		if msg, _ := probe["connect_error"].(string); !strings.Contains(strings.ToLower(msg), "authoriz") {
			t.Fatalf("the tool failed to connect, but not at authentication: %q", msg)
		}
	})
}
