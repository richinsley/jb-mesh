package node

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func startAuthzServer(t *testing.T, authz Authorization) *natsServerHandle {
	t.Helper()
	prepared, err := prepareAuthorization(authz)
	if err != nil {
		t.Fatalf("prepareAuthorization: %v", err)
	}
	ns, err := startEmbeddedNATS(embeddedNATSConfig{
		Authorization: prepared,
		Host:          "127.0.0.1",
		Port:          -1,
		LeafHost:      "127.0.0.1",
		LeafPort:      -1,
		Role:          "seed",
		StoreDir:      t.TempDir(),
		Logging:       LoggingConfig{Quiet: true},
	})
	if err != nil {
		t.Fatalf("startEmbeddedNATS: %v", err)
	}
	t.Cleanup(ns.Shutdown)
	return &natsServerHandle{
		url:      ns.ClientURL(),
		internal: prepared,
	}
}

type natsServerHandle struct {
	url      string
	internal Authorization
}

func connectUser(t *testing.T, url, user, password string, errCh chan error) (*nats.Conn, error) {
	t.Helper()
	opts := []nats.Option{
		nats.UserInfo(user, password),
		nats.NoReconnect(),
		nats.Timeout(2 * time.Second),
	}
	if errCh != nil {
		opts = append(opts, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errCh <- err
		}))
	}
	nc, err := nats.Connect(url, opts...)
	if err == nil {
		t.Cleanup(nc.Close)
	}
	return nc, err
}

func waitPermissionError(t *testing.T, ch <-chan error) string {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case err := <-ch:
			if err != nil && strings.Contains(strings.ToLower(err.Error()), "permission") {
				return err.Error()
			}
		case <-deadline:
			t.Fatal("timed out waiting for NATS permission error")
		}
	}
}

func TestTypedAuthorizationEnforcesPrincipalSubtree(t *testing.T) {
	h := startAuthzServer(t, Authorization{
		Enabled: true,
		Principals: []AuthorizedPrincipal{{
			PrincipalID:    "alice",
			Password:       "alice-secret",
			PublishAllow:   []string{"poppi.mesh.my-host.op.alice.>"},
			SubscribeAllow: []string{"poppi.mesh.my-host.op.alice.>"},
		}},
	})

	errCh := make(chan error, 8)
	alice, err := connectUser(t, h.url, "alice", "alice-secret", errCh)
	if err != nil {
		t.Fatalf("alice connect: %v", err)
	}
	sub, err := alice.SubscribeSync("poppi.mesh.my-host.op.alice.inbox")
	if err != nil {
		t.Fatalf("alice subscribe allowed subtree: %v", err)
	}
	if err := alice.Flush(); err != nil {
		t.Fatalf("alice flush subscribe: %v", err)
	}
	if err := alice.Publish("poppi.mesh.my-host.op.alice.inbox", []byte("ok")); err != nil {
		t.Fatalf("alice publish allowed subtree: %v", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("alice did not receive allowed message: %v", err)
	}

	if err := alice.Publish("poppi.mesh.my-host.op.bob.inbox", []byte("no")); err != nil {
		t.Fatalf("publish returns asynchronously; got immediate err: %v", err)
	}
	alice.Flush()
	waitPermissionError(t, errCh)
}

func TestTypedAuthorizationDeniesUndeclaredAndWrongCredential(t *testing.T) {
	h := startAuthzServer(t, Authorization{
		Enabled: true,
		Principals: []AuthorizedPrincipal{{
			PrincipalID:    "alice",
			Password:       "alice-secret",
			PublishAllow:   []string{"poppi.mesh.my-host.op.alice.>"},
			SubscribeAllow: []string{"poppi.mesh.my-host.op.alice.>"},
		}},
	})

	if _, err := connectUser(t, h.url, "alice", "wrong-secret", nil); err == nil {
		t.Fatal("alice connected with the wrong credential")
	}
	if _, err := connectUser(t, h.url, "mallory", "mallory-secret", nil); err == nil {
		t.Fatal("undeclared principal connected")
	}
	if _, err := connectUser(t, h.url, h.internal.InternalUsername, h.internal.InternalPassword, nil); err != nil {
		t.Fatalf("internal generated user should connect: %v", err)
	}
}

func TestTypedAuthorizationEmptyAdmitsNoExternalPrincipal(t *testing.T) {
	h := startAuthzServer(t, Authorization{Enabled: true})
	if _, err := nats.Connect(h.url, nats.NoReconnect(), nats.Timeout(2*time.Second)); err == nil {
		t.Fatal("anonymous client connected to empty typed authorization")
	}
	if _, err := connectUser(t, h.url, "anyone", "anything", nil); err == nil {
		t.Fatal("undeclared user connected to empty typed authorization")
	}
	if _, err := connectUser(t, h.url, h.internal.InternalUsername, h.internal.InternalPassword, nil); err != nil {
		t.Fatalf("internal generated user should connect: %v", err)
	}
}

func TestTypedAuthorizationEmptyDirectionsDenyAll(t *testing.T) {
	const ownSubject = "repl.alice.event"
	const otherSubject = "repl.bob.event"
	const privateSubject = "host.private.event"

	t.Run("subscribe", func(t *testing.T) {
		h := startAuthzServer(t, Authorization{
			Enabled: true,
			Principals: []AuthorizedPrincipal{{
				PrincipalID:  "alice",
				Password:     "alice-secret",
				PublishAllow: []string{"repl.alice.>"},
			}},
		})
		errs := make(chan error, 8)
		alice, err := connectUser(t, h.url, "alice", "alice-secret", errs)
		if err != nil {
			t.Fatalf("connect publish-only principal: %v", err)
		}
		internal, err := connectUser(t, h.url, h.internal.InternalUsername, h.internal.InternalPassword, nil)
		if err != nil {
			t.Fatalf("connect internal principal: %v", err)
		}
		for _, subject := range []string{ownSubject, otherSubject, privateSubject} {
			sub, err := alice.SubscribeSync(subject)
			if err != nil {
				t.Fatalf("submit forbidden subscription: %v", err)
			}
			if err := alice.FlushTimeout(2 * time.Second); err != nil {
				t.Fatalf("flush forbidden subscription: %v", err)
			}
			waitPermissionError(t, errs)
			if err := internal.Publish(subject, []byte("private")); err != nil {
				t.Fatalf("internal publish: %v", err)
			}
			if err := internal.FlushTimeout(2 * time.Second); err != nil {
				t.Fatalf("internal flush: %v", err)
			}
			if msg, err := sub.NextMsg(150 * time.Millisecond); err == nil {
				t.Fatalf("publish-only principal received %q on %s", msg.Data, subject)
			}
		}
		allowed, err := internal.SubscribeSync(ownSubject)
		if err != nil {
			t.Fatalf("internal subscribe to allowed publish: %v", err)
		}
		if err := internal.FlushTimeout(2 * time.Second); err != nil {
			t.Fatalf("flush internal subscription: %v", err)
		}
		if err := alice.Publish(ownSubject, []byte("allowed")); err != nil {
			t.Fatalf("publish-only principal lost its publish grant: %v", err)
		}
		if err := alice.FlushTimeout(2 * time.Second); err != nil {
			t.Fatalf("flush allowed publish: %v", err)
		}
		if _, err := allowed.NextMsg(2 * time.Second); err != nil {
			t.Fatalf("allowed publish was not delivered: %v", err)
		}
	})

	t.Run("publish", func(t *testing.T) {
		h := startAuthzServer(t, Authorization{
			Enabled: true,
			Principals: []AuthorizedPrincipal{{
				PrincipalID:    "alice",
				Password:       "alice-secret",
				PublishAllow:   []string{},
				SubscribeAllow: []string{"repl.alice.>"},
			}},
		})
		errs := make(chan error, 8)
		alice, err := connectUser(t, h.url, "alice", "alice-secret", errs)
		if err != nil {
			t.Fatalf("connect subscribe-only principal: %v", err)
		}
		internal, err := connectUser(t, h.url, h.internal.InternalUsername, h.internal.InternalPassword, nil)
		if err != nil {
			t.Fatalf("connect internal principal: %v", err)
		}
		for _, subject := range []string{ownSubject, otherSubject, privateSubject} {
			sub, err := internal.SubscribeSync(subject)
			if err != nil {
				t.Fatalf("internal subscribe: %v", err)
			}
			if err := internal.FlushTimeout(2 * time.Second); err != nil {
				t.Fatalf("internal subscribe flush: %v", err)
			}
			if err := alice.Publish(subject, []byte("forbidden")); err != nil {
				t.Fatalf("submit forbidden publish: %v", err)
			}
			if err := alice.FlushTimeout(2 * time.Second); err != nil {
				t.Fatalf("flush forbidden publish: %v", err)
			}
			waitPermissionError(t, errs)
			if msg, err := sub.NextMsg(150 * time.Millisecond); err == nil {
				t.Fatalf("subscribe-only principal published %q on %s", msg.Data, subject)
			}
		}
		allowed, err := alice.SubscribeSync(ownSubject)
		if err != nil {
			t.Fatalf("subscribe-only principal lost its subscribe grant: %v", err)
		}
		if err := alice.FlushTimeout(2 * time.Second); err != nil {
			t.Fatalf("flush allowed subscription: %v", err)
		}
		if err := internal.Publish(ownSubject, []byte("allowed")); err != nil {
			t.Fatalf("internal publish to allowed subscription: %v", err)
		}
		if _, err := allowed.NextMsg(2 * time.Second); err != nil {
			t.Fatalf("allowed subscription did not receive: %v", err)
		}
	})
}

func TestReloadAuthorizationAddsAndRevokesPrincipalsWithoutStrandingSeed(t *testing.T) {
	alice := AuthorizedPrincipal{
		PrincipalID:    "alice",
		Password:       "alice-secret",
		PublishAllow:   []string{"tools.>"},
		SubscribeAllow: []string{"tools.>"},
	}
	bob := AuthorizedPrincipal{
		PrincipalID:    "bob",
		Password:       "bob-secret",
		PublishAllow:   []string{"tools.>"},
		SubscribeAllow: []string{"tools.>"},
	}
	n, err := New(Config{
		HomeDir:       t.TempDir(),
		NodeName:      "auth-reload-seed",
		Role:          "seed",
		NoMDNS:        true,
		EmbedHost:     "127.0.0.1",
		EmbedPort:     -1,
		LeafHost:      "127.0.0.1",
		LeafPort:      -1,
		Authorization: Authorization{Enabled: true, Principals: []AuthorizedPrincipal{alice}},
		Logging:       LoggingConfig{Quiet: true},
	})
	if err != nil {
		t.Fatalf("start typed seed: %v", err)
	}
	t.Cleanup(n.Close)
	url := n.Mesh().Conn().ConnectedUrl()
	aliceConn, err := connectUser(t, url, alice.PrincipalID, alice.Password, nil)
	if err != nil {
		t.Fatalf("alice initial connect: %v", err)
	}
	if _, err := connectUser(t, url, bob.PrincipalID, bob.Password, nil); err == nil {
		t.Fatal("bob connected before authorization reload")
	}

	if err := n.ReloadAuthorization(Authorization{Enabled: true, Principals: []AuthorizedPrincipal{alice, bob}}); err != nil {
		t.Fatalf("add bob: %v", err)
	}
	if _, err := connectUser(t, url, bob.PrincipalID, bob.Password, nil); err != nil {
		t.Fatalf("bob connect after reload: %v", err)
	}
	if !n.Mesh().Conn().IsConnected() {
		t.Fatal("seed internal mesh client was stranded by authorization reload")
	}

	if err := n.ReloadAuthorization(Authorization{Enabled: true, Principals: []AuthorizedPrincipal{bob}}); err != nil {
		t.Fatalf("remove alice: %v", err)
	}
	if err := aliceConn.FlushTimeout(2 * time.Second); err == nil {
		t.Fatal("removed alice connection remained authorized after reload")
	}
	if _, err := connectUser(t, url, alice.PrincipalID, alice.Password, nil); err == nil {
		t.Fatal("removed alice reconnected after reload")
	}
	if !n.Mesh().Conn().IsConnected() {
		t.Fatal("seed internal mesh client was disconnected by revocation reload")
	}
}

func TestReloadAuthorizationEmptyDirectionDoesNotReopen(t *testing.T) {
	for _, direction := range []string{"subscribe", "publish"} {
		t.Run(direction, func(t *testing.T) {
			alice := AuthorizedPrincipal{
				PrincipalID:    "alice",
				Password:       "alice-secret",
				PublishAllow:   []string{"repl.alice.>"},
				SubscribeAllow: []string{"repl.alice.>"},
			}
			n, err := New(Config{
				HomeDir:       t.TempDir(),
				NodeName:      "auth-narrow-seed",
				Role:          "seed",
				NoMDNS:        true,
				EmbedHost:     "127.0.0.1",
				EmbedPort:     -1,
				LeafHost:      "127.0.0.1",
				LeafPort:      -1,
				Authorization: Authorization{Enabled: true, Principals: []AuthorizedPrincipal{alice}},
				Logging:       LoggingConfig{Quiet: true},
			})
			if err != nil {
				t.Fatalf("start typed seed: %v", err)
			}
			t.Cleanup(n.Close)
			internal := n.Mesh().Conn()
			old, err := connectUser(t, internal.ConnectedUrl(), alice.PrincipalID, alice.Password, nil)
			if err != nil {
				t.Fatalf("initial alice connect: %v", err)
			}
			const subject = "repl.alice.event"
			var oldSub *nats.Subscription
			if direction == "subscribe" {
				oldSub, err = old.SubscribeSync(subject)
				if err != nil {
					t.Fatalf("initial allowed subscription: %v", err)
				}
				if err := old.FlushTimeout(2 * time.Second); err != nil {
					t.Fatalf("flush initial subscription: %v", err)
				}
				alice.SubscribeAllow = nil
			} else {
				alice.PublishAllow = nil
			}
			internalSub, err := internal.SubscribeSync(subject)
			if err != nil {
				t.Fatalf("internal subscribe: %v", err)
			}
			defer internalSub.Unsubscribe()
			if err := internal.FlushTimeout(2 * time.Second); err != nil {
				t.Fatalf("flush internal subscription: %v", err)
			}
			if err := n.ReloadAuthorization(Authorization{Enabled: true, Principals: []AuthorizedPrincipal{alice}}); err != nil {
				t.Fatalf("narrow %s grant: %v", direction, err)
			}
			if direction == "subscribe" {
				if err := internal.Publish(subject, []byte("after reload")); err != nil {
					t.Fatalf("internal publish: %v", err)
				}
				if err := internal.FlushTimeout(2 * time.Second); err != nil {
					t.Fatalf("flush internal publish: %v", err)
				}
				if msg, err := oldSub.NextMsg(150 * time.Millisecond); err == nil {
					t.Fatalf("old subscription survived narrowed grant: %q", msg.Data)
				}
			} else {
				// NATS may disconnect or update the old client on reload. Neither
				// outcome may deliver a publish after its grant is removed.
				_ = old.Publish(subject, []byte("old client"))
				_ = old.FlushTimeout(2 * time.Second)
				if msg, err := internalSub.NextMsg(150 * time.Millisecond); err == nil {
					t.Fatalf("old client published after narrowed grant: %q", msg.Data)
				}
			}
			errs := make(chan error, 8)
			fresh, err := connectUser(t, internal.ConnectedUrl(), alice.PrincipalID, alice.Password, errs)
			if err != nil {
				t.Fatalf("connect after narrowing: %v", err)
			}
			if direction == "subscribe" {
				if _, err := fresh.SubscribeSync(subject); err != nil {
					t.Fatalf("submit forbidden subscription after reload: %v", err)
				}
			} else {
				if err := fresh.Publish(subject, []byte("new client")); err != nil {
					t.Fatalf("submit forbidden publish after reload: %v", err)
				}
			}
			if err := fresh.FlushTimeout(2 * time.Second); err != nil {
				t.Fatalf("flush forbidden operation after reload: %v", err)
			}
			waitPermissionError(t, errs)
			if direction == "publish" {
				if msg, err := internalSub.NextMsg(150 * time.Millisecond); err == nil {
					t.Fatalf("new client published after narrowed grant: %q", msg.Data)
				}
			}
			if !internal.IsConnected() {
				t.Fatal("seed internal mesh client disconnected by narrowing")
			}
		})
	}
}

func TestTypedAuthorizationValidation(t *testing.T) {
	if _, err := New(Config{
		HomeDir:       t.TempDir(),
		NATSUrl:       "nats://127.0.0.1:4222",
		Authorization: Authorization{Enabled: true},
		Logging:       LoggingConfig{Quiet: true},
	}); err == nil || !strings.Contains(err.Error(), "external NATS") {
		t.Fatalf("typed authorization with external NATS URL accepted: %v", err)
	}
	if _, err := New(Config{
		HomeDir:       t.TempDir(),
		Token:         "global-token",
		Authorization: Authorization{Enabled: true},
		Logging:       LoggingConfig{Quiet: true},
	}); err == nil || !strings.Contains(err.Error(), "global token") {
		t.Fatalf("typed authorization with token accepted: %v", err)
	}
	if _, err := prepareAuthorization(Authorization{
		Enabled: true,
		Principals: []AuthorizedPrincipal{{
			PrincipalID:    "alice",
			Password:       "secret",
			PublishAllow:   []string{">"},
			SubscribeAllow: []string{"poppi.mesh.my-host.op.alice.>"},
		}},
	}); err == nil || !strings.Contains(err.Error(), "all-subject") {
		t.Fatalf("all-subject principal grant accepted: %v", err)
	}
	if _, err := prepareAuthorization(Authorization{
		Enabled: true,
		Principals: []AuthorizedPrincipal{
			{PrincipalID: "alice", Password: "one"},
			{PrincipalID: "alice", Password: "two"},
		},
	}); err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("duplicate principal accepted: %v", err)
	}
	if _, err := prepareAuthorization(Authorization{
		Enabled:          true,
		InternalUsername: "_jbmesh_internal",
		InternalPassword: "internal-secret",
		Principals: []AuthorizedPrincipal{{
			PrincipalID:    "_jbmesh_internal",
			Password:       "external-secret",
			PublishAllow:   []string{"poppi.mesh.my-host.op.internal.>"},
			SubscribeAllow: []string{"poppi.mesh.my-host.op.internal.>"},
		}},
	}); err == nil || !strings.Contains(err.Error(), "internal user") {
		t.Fatalf("internal principal collision accepted: %v", err)
	}
}
