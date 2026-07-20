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
