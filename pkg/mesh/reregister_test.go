package mesh

import (
	"net"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// restartableNATS is a loopback server that can be stopped and started again
// on the same port, so a client connected to it goes through a real reconnect.
type restartableNATS struct {
	t    *testing.T
	port int
	ns   *natsserver.Server
}

func startRestartableNATS(t *testing.T) *restartableNATS {
	t.Helper()
	r := &restartableNATS{t: t, port: -1}
	r.start()
	t.Cleanup(func() { r.ns.Shutdown() })
	return r
}

func (r *restartableNATS) start() {
	r.t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: r.port, NoLog: true, NoSigs: true})
	if err != nil {
		r.t.Fatalf("NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		r.t.Fatal("NATS server did not start")
	}
	r.ns = ns
	r.port = ns.Addr().(*net.TCPAddr).Port
}

func (r *restartableNATS) restart() {
	r.t.Helper()
	r.ns.Shutdown()
	r.ns.WaitForShutdown()
	r.start()
}

// repliesOn counts the answers to one request on subject within wait.
func repliesOn(t *testing.T, nc *nats.Conn, subject string, wait time.Duration) (n int, last string) {
	t.Helper()
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.PublishRequest(subject, inbox, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(wait)
	for {
		msg, err := sub.NextMsg(time.Until(deadline))
		if err != nil {
			return n, last
		}
		if len(msg.Data) == 0 { // the server's no-responders status
			continue
		}
		n++
		last = string(msg.Data)
	}
}

// TestUnregisteredToolIsNotRestoredByAReconnect: what a reconnect restores is
// what is registered now. A tool that was unregistered stays gone, and a tool
// that was unregistered and registered again comes back once, with the
// handler it was last registered with.
func TestUnregisteredToolIsNotRestoredByAReconnect(t *testing.T) {
	server := startRestartableNATS(t)
	m, err := New(Config{NATSUrl: server.ns.ClientURL(), NodeName: "n1", Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	handler := func(tag string) ToolHandler {
		return func(CallRequest, string, map[string]interface{}) (interface{}, error) { return tag, nil }
	}
	for _, reg := range []struct{ name, tag string }{{"gone", "gone-1"}, {"replaced", "old"}, {"kept", "kept-1"}} {
		if err := m.RegisterTool(reg.name, "1.0.0", "", []string{"m"}, handler(reg.tag)); err != nil {
			t.Fatal(err)
		}
	}
	if !m.UnregisterTool("gone") {
		t.Fatal("gone was not registered")
	}
	if !m.UnregisterTool("replaced") {
		t.Fatal("replaced was not registered")
	}
	if err := m.RegisterTool("replaced", "1.0.1", "", []string{"m"}, handler("new")); err != nil {
		t.Fatal(err)
	}

	check := func(when string) {
		t.Helper()
		m.mu.Lock()
		saved := map[string]int{}
		for _, reg := range m.registrations {
			saved[reg.name]++
		}
		services, nodeSubs := len(m.services), len(m.nodeSubs)
		m.mu.Unlock()
		if len(saved) != 2 || saved["kept"] != 1 || saved["replaced"] != 1 {
			t.Fatalf("%s: saved registrations %v, want kept and replaced once each", when, saved)
		}
		if services != 2 || nodeSubs != 2 {
			t.Fatalf("%s: %d services and %d node subscriptions, want 2 and 2", when, services, nodeSubs)
		}

		probe, err := nats.Connect(server.ns.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		defer probe.Close()
		for subject, want := range map[string]string{
			"node.n1.tools.kept.m":     `"result":"kept-1"`,
			"tools.kept.m":             `"result":"kept-1"`,
			"node.n1.tools.replaced.m": `"result":"new"`,
			"tools.replaced.m":         `"result":"new"`,
		} {
			n, last := repliesOn(t, probe, subject, 400*time.Millisecond)
			if n != 1 || !strings.Contains(last, want) {
				t.Fatalf("%s: %s gave %d answers, last %s; want one with %s", when, subject, n, last, want)
			}
		}
		for _, subject := range []string{"node.n1.tools.gone.m", "tools.gone.m"} {
			if n, last := repliesOn(t, probe, subject, 400*time.Millisecond); n != 0 {
				t.Fatalf("%s: %s is answered (%d, %s) after the tool was unregistered", when, subject, n, last)
			}
		}
	}
	check("before a reconnect")

	server.restart()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if m.Conn().IsConnected() {
			if res, err := m.Call("kept", "m", nil, time.Second); err == nil && res.OK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the mesh did not come back after the server restarted")
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // let the re-registration finish
	check("after a reconnect")
}
