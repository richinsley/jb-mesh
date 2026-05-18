package node

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestEmbeddedNATSWebsocketJetStreamObjectStore(t *testing.T) {
	tmp := t.TempDir()
	server, err := startEmbeddedNATS(embeddedNATSConfig{
		StoreDir:      tmp,
		Port:          -1,
		NodeName:      "ws-test",
		Role:          "seed",
		LeafPort:      -1,
		WebsocketHost: "127.0.0.1",
		WebsocketPort: -1,
	})
	if err != nil {
		t.Fatalf("start embedded NATS: %v", err)
	}
	defer server.Shutdown()

	wsURL := server.WebsocketURL()
	if wsURL == "" {
		t.Fatal("expected websocket URL")
	}

	nc, err := nats.Connect(wsURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("connect websocket NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream over websocket: %v", err)
	}

	bucket, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "ws_filestore_test"})
	if err != nil {
		t.Fatalf("create object store over websocket: %v", err)
	}

	if _, err := bucket.PutString("hello.txt", "hello over websocket"); err != nil {
		t.Fatalf("put object over websocket: %v", err)
	}
	got, err := bucket.GetString("hello.txt")
	if err != nil {
		t.Fatalf("get object over websocket: %v", err)
	}
	if got != "hello over websocket" {
		t.Fatalf("object contents = %q", got)
	}
}
