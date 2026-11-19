package node

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/richinsley/jb-mesh/pkg/mesh"
)

// A principal held to "_INBOX.<principal>.>" gets its own replies through
// mesh.Config.InboxPrefix, and a second admitted principal can neither
// subscribe to those replies nor receive them.
func TestInboxPrefixKeepsRepliesFromOtherPrincipals(t *testing.T) {
	srv := startAuthzServer(t, Authorization{
		Enabled: true,
		Principals: []AuthorizedPrincipal{
			{PrincipalID: "leaf", Password: "leaf-pass", PublishAllow: []string{"_INBOX.>"}, SubscribeAllow: []string{"node.n.tools.>"}},
			{PrincipalID: "alice", Password: "alice-pass", PublishAllow: []string{"node.n.tools.>"}, SubscribeAllow: []string{"_INBOX.alice.>"}},
			{PrincipalID: "bob", Password: "bob-pass", PublishAllow: []string{"node.n.tools.>"}, SubscribeAllow: []string{"_INBOX.bob.>"}},
		},
	})

	leaf, err := connectUser(t, srv.url, "leaf", "leaf-pass", nil)
	if err != nil {
		t.Fatalf("leaf connect: %v", err)
	}
	if _, err := leaf.Subscribe("node.n.tools.echo.say", func(m *nats.Msg) {
		_ = m.Respond([]byte("reply-for:" + string(m.Data)))
	}); err != nil {
		t.Fatalf("leaf subscribe: %v", err)
	}
	if err := leaf.Flush(); err != nil {
		t.Fatalf("leaf flush: %v", err)
	}

	// Bob tries every subscription that would see Alice's replies.
	bobErrs := make(chan error, 8)
	bob, err := connectUser(t, srv.url, "bob", "bob-pass", bobErrs)
	if err != nil {
		t.Fatalf("bob connect: %v", err)
	}
	overheard := make(chan string, 8)
	probes := []string{"_INBOX.>", "_INBOX.alice.>", "_INBOX.*.>", ">", "_INBOX.alice.*"}
	for _, subject := range probes {
		if _, err := bob.Subscribe(subject, func(m *nats.Msg) { overheard <- m.Subject }); err != nil {
			t.Fatalf("bob subscribe %s: %v", subject, err)
		}
	}
	if err := bob.Flush(); err != nil {
		t.Fatalf("bob flush: %v", err)
	}
	for range probes {
		select {
		case err := <-bobErrs:
			if !strings.Contains(strings.ToLower(err.Error()), "permissions violation") {
				t.Fatalf("bob's probe failed for another reason: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the server did not refuse one of bob's probe subscriptions")
		}
	}

	alice, err := mesh.Connect(mesh.Config{NATSUrl: srv.url, Username: "alice", Password: "alice-pass", InboxPrefix: "_INBOX.alice"}, nats.NoReconnect())
	if err != nil {
		t.Fatalf("alice connect: %v", err)
	}
	defer alice.Close()

	// The plain request path (shared wildcard reply subscription).
	msg, err := alice.Request("node.n.tools.echo.say", []byte("one"), 2*time.Second)
	if err != nil || string(msg.Data) != "reply-for:one" {
		t.Fatalf("alice's request: %v %v", msg, err)
	}
	// The stream path (a dedicated reply inbox).
	inbox := alice.NewRespInbox()
	if !strings.HasPrefix(inbox, "_INBOX.alice.") {
		t.Fatalf("reply inbox %q is outside alice's prefix", inbox)
	}
	sub, err := alice.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("alice inbox subscribe: %v", err)
	}
	if err := alice.PublishRequest("node.n.tools.echo.say", inbox, []byte("two")); err != nil {
		t.Fatalf("alice publish: %v", err)
	}
	if msg, err := sub.NextMsg(2 * time.Second); err != nil || string(msg.Data) != "reply-for:two" {
		t.Fatalf("alice's stream reply: %v %v", msg, err)
	}

	select {
	case subject := <-overheard:
		t.Fatalf("bob received a reply addressed to %s", subject)
	case <-time.After(300 * time.Millisecond):
	}

	// Bob's own replies still work under his own prefix.
	bobScoped, err := mesh.Connect(mesh.Config{NATSUrl: srv.url, Username: "bob", Password: "bob-pass", InboxPrefix: "_INBOX.bob"}, nats.NoReconnect())
	if err != nil {
		t.Fatalf("bob scoped connect: %v", err)
	}
	defer bobScoped.Close()
	if msg, err := bobScoped.Request("node.n.tools.echo.say", []byte("three"), 2*time.Second); err != nil || string(msg.Data) != "reply-for:three" {
		t.Fatalf("bob's own request: %v %v", msg, err)
	}
}

// A client that keeps the default inbox on a scoped server fails closed: the
// server refuses its reply subscription and the request gets no reply.
func TestDefaultInboxFailsClosedOnScopedServer(t *testing.T) {
	srv := startAuthzServer(t, Authorization{
		Enabled: true,
		Principals: []AuthorizedPrincipal{
			{PrincipalID: "leaf", Password: "leaf-pass", PublishAllow: []string{"_INBOX.>"}, SubscribeAllow: []string{"node.n.tools.>"}},
			{PrincipalID: "alice", Password: "alice-pass", PublishAllow: []string{"node.n.tools.>"}, SubscribeAllow: []string{"_INBOX.alice.>"}},
		},
	})
	leaf, err := connectUser(t, srv.url, "leaf", "leaf-pass", nil)
	if err != nil {
		t.Fatalf("leaf connect: %v", err)
	}
	if _, err := leaf.Subscribe("node.n.tools.echo.say", func(m *nats.Msg) { _ = m.Respond([]byte("x")) }); err != nil {
		t.Fatalf("leaf subscribe: %v", err)
	}
	_ = leaf.Flush()

	errs := make(chan error, 4)
	legacy, err := mesh.Connect(mesh.Config{NATSUrl: srv.url, Username: "alice", Password: "alice-pass"},
		nats.NoReconnect(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err }))
	if err != nil {
		t.Fatalf("legacy connect: %v", err)
	}
	defer legacy.Close()
	if _, err := legacy.Request("node.n.tools.echo.say", []byte("one"), 500*time.Millisecond); err == nil {
		t.Fatal("a default-inbox request must not receive a reply on a scoped server")
	}
	select {
	case err := <-errs:
		if !strings.Contains(strings.ToLower(err.Error()), "permissions violation") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the server did not report the refused default-inbox subscription")
	}
}

func TestValidateInboxPrefix(t *testing.T) {
	for _, ok := range []string{"_INBOX.alice", "_INBOX.leaf-n1", "X"} {
		if err := mesh.ValidateInboxPrefix(ok); err != nil {
			t.Fatalf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "_INBOX.", ".a", "_INBOX.*", "_INBOX.>", "_INBOX.a b", "a..b"} {
		if err := mesh.ValidateInboxPrefix(bad); err == nil {
			t.Fatalf("%q must be refused", bad)
		}
	}
	if _, err := mesh.Connect(mesh.Config{NATSUrl: "nats://127.0.0.1:1", InboxPrefix: "_INBOX.*"}); err == nil || !strings.Contains(err.Error(), "inbox prefix") {
		t.Fatalf("Connect must refuse a wildcard prefix before dialing, got %v", err)
	}
}

// The node builds its mesh client configuration in one place, so the reconnect
// after a demotion carries the same client identity as the first connection:
// token, username, password, inbox prefix and WebSocket settings. Before, the
// reconnect carried the token only, so a seed with typed authorization refused
// it, and a seed that scopes replies would have refused its reply inboxes.
func TestMeshConfigIsTheSameForFirstConnectAndDemotionReconnect(t *testing.T) {
	n := &Node{
		nodeName:          "leafish",
		token:             "tok",
		clientUsername:    "alice",
		clientPassword:    "pw",
		clientInboxPrefix: "_INBOX.alice",
		clientWebSocket:   mesh.NATSWebSocketConfig{},
	}
	first := n.meshConfig("nats://127.0.0.1:1", "internal-user", "internal-pw")
	again := n.meshConfig("nats://127.0.0.1:2", "", "")
	for name, c := range map[string]mesh.Config{"first": first, "reconnect": again} {
		if c.NodeName != "leafish" || c.Token != "tok" || c.Username != "alice" || c.Password != "pw" || c.InboxPrefix != "_INBOX.alice" {
			t.Fatalf("%s connection config lost part of the client identity: %+v", name, c)
		}
	}
	if first.NATSUrl == again.NATSUrl {
		t.Fatal("the reconnect did not take the new seed's URL")
	}

	// With no client identity configured, the first connection may use the
	// embedded server's internal credential; the reconnect to ANOTHER seed
	// must not offer it, and so is refused there under typed authorization.
	bare := &Node{nodeName: "leafish"}
	if c := bare.meshConfig("nats://127.0.0.1:1", "internal-user", "internal-pw"); c.Username != "internal-user" || c.Password != "internal-pw" {
		t.Fatalf("first connection without a client identity: %+v", c)
	}
	if c := bare.meshConfig("nats://127.0.0.1:2", "", ""); c.Username != "" || c.Password != "" || c.InboxPrefix != "" {
		t.Fatalf("reconnect offered a credential it does not hold on the other seed: %+v", c)
	}
}
