package node

import (
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/mesh"
	"github.com/richinsley/jb-mesh/pkg/replytoken"
)

// A stream that fails before its first frame is answered with one terminal
// error frame. That frame carries the request's reply token like any other,
// so a caller that requires the token receives it.
func TestStreamErrorFrameCarriesTheRequestsReplyToken(t *testing.T) {
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	defer ns.Shutdown()
	m, err := mesh.New(mesh.Config{NATSUrl: ns.ClientURL(), NodeName: "n1", Logging: mesh.LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	n := &Node{mesh: m, nodeName: "n1"}

	client, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reply := client.NewRespInbox()
	sub, err := client.SubscribeSync(reply)
	if err != nil {
		t.Fatal(err)
	}
	// The frame is published from another connection; the subscription must
	// be known to the server first.
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
	token, err := replytoken.New()
	if err != nil {
		t.Fatal(err)
	}
	n.publishStreamErrorFrame(&nats.Msg{Reply: reply, Header: replytoken.EchoToken(token)}, "the tool is not running")
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !replytoken.Matches(msg, token) {
		t.Fatalf("the error frame carries token %q, want the request's", replytoken.Of(msg))
	}
	var frame mesh.StreamFrame
	if err := json.Unmarshal(msg.Data, &frame); err != nil || !frame.Done || frame.Error != "the tool is not running" || frame.Node != "n1" {
		t.Fatalf("frame = %+v, %v", frame, err)
	}
	// A request with no reply subject is not answered and does not panic.
	n.publishStreamErrorFrame(&nats.Msg{}, "x")
	n.publishStreamErrorFrame(nil, "x")
}
