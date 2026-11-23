package node

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestEveryFileProviderAnswersEveryRequest records how the file store behaves when more than
// one node serves it: a seed and a leaf joined to it, each with file serving on, asked through
// each in turn.
//
// The handlers are plain subscriptions, not a queue group, so every provider receives every
// request, acts on it against its own handle on the store, and answers. A caller that makes
// an ordinary request sees only whichever answer arrives first. Here the two nodes share one
// store, so the answers agree and the store ends in the state the caller expects. Nothing in
// the protocol makes that so: providers whose stores are not shared would each act on their
// own, and the one answer a caller sees would speak for one of them.
func TestEveryFileProviderAnswersEveryRequest(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	leafPort := l.Addr().(*net.TCPAddr).Port
	l.Close()
	seed, err := New(Config{HomeDir: t.TempDir(), NodeName: "seed", Role: "seed", NoMDNS: true,
		EmbedHost: "127.0.0.1", EmbedPort: -1, LeafHost: "127.0.0.1", LeafPort: leafPort, Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatalf("start seed: %v", err)
	}
	t.Cleanup(seed.Close)
	leaf, err := New(Config{HomeDir: t.TempDir(), NodeName: "leaf", NoMDNS: true, Role: "leaf",
		LeafURL:   fmt.Sprintf("nats-leaf://127.0.0.1:%d", leafPort),
		EmbedHost: "127.0.0.1", EmbedPort: -1, Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatalf("start leaf: %v", err)
	}
	t.Cleanup(leaf.Close)

	type reply struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
		Files []struct {
			Key string `json:"key"`
		} `json:"files"`
	}
	// every answer one request gets, not only the first
	all := func(nc *nats.Conn, subject, body string) []reply {
		t.Helper()
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if err := nc.PublishRequest(subject, inbox, []byte(body)); err != nil {
			t.Fatal(err)
		}
		var out []reply
		for {
			msg, err := sub.NextMsg(1500 * time.Millisecond)
			if err != nil {
				return out
			}
			if len(msg.Data) == 0 {
				continue
			}
			var r reply
			if err := json.Unmarshal(msg.Data, &r); err != nil {
				t.Fatalf("%s: %v", subject, err)
			}
			out = append(out, r)
		}
	}

	for _, through := range []struct {
		name string
		url  string
	}{{"the seed", seed.Mesh().Conn().ConnectedUrl()}, {"the leaf", leaf.Mesh().Conn().ConnectedUrl()}} {
		nc, err := nats.Connect(through.url)
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		// both providers are reachable from here once the leaf has joined
		deadline := time.Now().Add(15 * time.Second)
		for len(all(nc, "files.list", `{"prefix":"probe/"}`)) < 2 {
			if time.Now().After(deadline) {
				t.Fatalf("through %s: two file providers never answered", through.name)
			}
		}
		key := "probe/" + through.name[4:] + ".txt"
		expect := func(what string, got []reply, ok bool, files int) {
			t.Helper()
			if len(got) != 2 {
				t.Fatalf("through %s, %s: %d answers, want one from each of the two providers", through.name, what, len(got))
			}
			for i, r := range got {
				if r.OK != ok || len(r.Files) != files {
					t.Fatalf("through %s, %s: answer %d is ok=%v files=%d error=%q; want ok=%v files=%d",
						through.name, what, i, r.OK, len(r.Files), r.Error, ok, files)
				}
			}
		}
		expect("put", all(nc, "files.put", `{"key":"`+key+`","data":"aGVsbG8=","content_type":"text/plain"}`), true, 0)
		expect("list after put", all(nc, "files.list", `{"prefix":"probe/"}`), true, 1)
		expect("get", all(nc, "files.get", `{"key":"`+key+`"}`), true, 0)
		expect("delete", all(nc, "files.delete", `{"key":"`+key+`"}`), true, 0)
		expect("list after delete", all(nc, "files.list", `{"prefix":"probe/"}`), true, 0)
		expect("get after delete", all(nc, "files.get", `{"key":"`+key+`"}`), false, 0)
		expect("delete of a key never stored", all(nc, "files.delete", `{"key":"probe/never-stored.txt"}`), false, 0)
	}
}
