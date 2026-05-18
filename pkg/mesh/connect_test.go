package mesh

import (
	"net/url"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

func TestPrepareConnectURLMergesQuery(t *testing.T) {
	got, err := prepareConnectURL("wss://example.com?existing=1", NATSWebSocketConfig{
		Query: url.Values{
			"token": {"abc"},
		},
	})
	if err != nil {
		t.Fatalf("prepareConnectURL: %v", err)
	}
	if got != "wss://example.com?existing=1&token=abc" && got != "wss://example.com?token=abc&existing=1" {
		t.Fatalf("unexpected URL %q", got)
	}
}

func TestPrepareConnectURLRejectsNonWebSocketURL(t *testing.T) {
	_, err := prepareConnectURL("nats://127.0.0.1:4222", NATSWebSocketConfig{ProxyPath: "/mesh/nats"})
	if err == nil {
		t.Fatal("expected error for non-websocket URL")
	}
}

func TestParseKeyValuePairs(t *testing.T) {
	got, err := ParseKeyValuePairs([]string{"Authorization=Bearer abc", "token=one", "token=two"})
	if err != nil {
		t.Fatalf("ParseKeyValuePairs: %v", err)
	}
	if len(got["token"]) != 2 || got["token"][0] != "one" || got["token"][1] != "two" {
		t.Fatalf("unexpected token values: %#v", got["token"])
	}
	if got["Authorization"][0] != "Bearer abc" {
		t.Fatalf("unexpected auth value: %#v", got["Authorization"])
	}
}

func TestConnectDirectWebSocketWithQueryOptions(t *testing.T) {
	ns := startTestWebSocketNATS(t)
	nc, err := Connect(Config{
		NATSUrl:  ns.WebsocketURL(),
		NodeName: "proxy-test",
		WebSocket: NATSWebSocketConfig{
			Query: url.Values{
				"token": {"demo-query"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Connect websocket with query options: %v", err)
	}
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("expected websocket connection")
	}
}

func startTestWebSocketNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
		Websocket: natsserver.WebsocketOpts{
			Host:  "127.0.0.1",
			Port:  -1,
			NoTLS: true,
		},
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS websocket server failed to start")
	}
	t.Cleanup(func() { ns.Shutdown() })
	return ns
}
