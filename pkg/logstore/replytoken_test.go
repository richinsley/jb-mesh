package logstore

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/replytoken"
)

// The log store's services answer on the requester's reply subject like any
// other responder, so they echo the request's reply token.
func TestLogStoreServicesEchoTheReplyToken(t *testing.T) {
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	defer ns.Shutdown()
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	store, err := NewStore(Config{StorageDir: t.TempDir(), Redact: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartHealthService(nc, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartQueryServices(nc); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	client, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	req := &replytoken.Requester{NC: client, Require: true}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Each service, with a body it accepts and one it refuses.
	for _, subject := range []string{"logstore.health", "logstore.query", "logstore.stats", "logstore.tail"} {
		for _, body := range []string{`{}`, `{not json`} {
			if _, err := req.Request(ctx, subject, []byte(body)); err != nil {
				t.Fatalf("%s %s: no token-carrying answer: %v", subject, body, err)
			}
		}
	}
	if n := req.Rejections(); n != 0 {
		t.Fatalf("%d log store answer(s) were rejected", n)
	}
}
