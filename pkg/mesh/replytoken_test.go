package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/richinsley/jb-mesh/pkg/filestore"
	"github.com/richinsley/jb-mesh/pkg/replytoken"
)

// replytoken_test.go: a requester with RequireReplyToken accepts a message as
// the answer to a call only if it carries that call's token. The adversary in
// these tests may publish to the victim's reply subject and may make an
// honest responder publish to it. Each test checks three things apart: the
// forged message reached the wire, the caller did not get it, and the
// rejection was counted.

type tokenLab struct {
	t        *testing.T
	url      string
	server   *Mesh // the honest node "n1"
	victim   *Mesh // requires the token
	attacker *nats.Conn
}

// tap records every message published to a subject pattern, so a test can
// show a forged message was really on the wire.
type tap struct {
	mu   sync.Mutex
	msgs []*nats.Msg
}

func (tp *tap) on(t *testing.T, nc *nats.Conn, subject string) {
	t.Helper()
	if _, err := nc.Subscribe(subject, func(m *nats.Msg) {
		tp.mu.Lock()
		tp.msgs = append(tp.msgs, m)
		tp.mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func (tp *tap) bodies() []string {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	out := make([]string, len(tp.msgs))
	for i, m := range tp.msgs {
		out[i] = string(m.Data)
	}
	return out
}

func newTokenLab(t *testing.T) *tokenLab {
	t.Helper()
	ns := startTestNATS(t)
	lab := &tokenLab{t: t, url: ns.ClientURL()}
	var err error
	if lab.server, err = New(Config{NATSUrl: lab.url, NodeName: "n1", Logging: LoggingConfig{Quiet: true}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lab.server.Close)
	if lab.victim, err = New(Config{NATSUrl: lab.url, NodeName: "victim", RequireReplyToken: true, Logging: LoggingConfig{Quiet: true}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lab.victim.Close)
	if lab.attacker, err = nats.Connect(lab.url); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lab.attacker.Close)
	return lab
}

// slowEcho registers a tool on the honest node whose "say" method waits, so
// an adversary has time to act while the call is in flight, then answers
// with the text it was given. "fail" returns an error.
func (lab *tokenLab) slowEcho(wait time.Duration) {
	lab.t.Helper()
	err := lab.server.RegisterTool("echo", "1.0.0", "echo", []string{"say", "fail", "quick"},
		func(_ CallRequest, method string, params map[string]interface{}) (interface{}, error) {
			if method == "quick" {
				// Answers at once, on its own subscription, so it can be
				// made to answer while a "say" is still in flight.
				return "said:" + params["text"].(string), nil
			}
			time.Sleep(wait)
			if method == "fail" {
				return nil, errors.New("the tool refused: " + params["text"].(string))
			}
			return "said:" + params["text"].(string), nil
		})
	if err != nil {
		lab.t.Fatal(err)
	}
	if err := lab.server.Conn().Flush(); err != nil {
		lab.t.Fatal(err)
	}
}

// replySubjectOf waits for the victim's request on subject and returns its
// reply subject, as an adversary that can see the request would learn it.
func (lab *tokenLab) spyReply(subject string) <-chan string {
	lab.t.Helper()
	ch := make(chan string, 1)
	if _, err := lab.attacker.Subscribe(subject, func(m *nats.Msg) {
		select {
		case ch <- m.Reply:
		default:
		}
	}); err != nil {
		lab.t.Fatal(err)
	}
	if err := lab.attacker.Flush(); err != nil {
		lab.t.Fatal(err)
	}
	return ch
}

func forgedResult(text string) []byte {
	data, _ := json.Marshal(CallResult{OK: true, Result: text, Node: "n1"})
	return data
}

func TestRequireReplyTokenRejectsAMessagePublishedToTheReplySubject(t *testing.T) {
	lab := newTokenLab(t)
	lab.slowEcho(250 * time.Millisecond)
	for _, tc := range []struct {
		name, method string
		forge        func(reply string) *nats.Msg
		wantOK       bool
		want         string
	}{
		{"an ordinary-looking reply with no token, sent first", "say",
			func(r string) *nats.Msg { return &nats.Msg{Subject: r, Data: forgedResult("FORGED")} }, true, "said:hello"},
		{"a reply with a made-up token", "say",
			func(r string) *nats.Msg {
				return &nats.Msg{Subject: r, Data: forgedResult("FORGED"), Header: nats.Header{replytoken.Header: []string{strings.Repeat("ab", replytoken.Size)}}}
			}, true, "said:hello"},
		{"an error-shaped reply, sent before the real error", "fail",
			func(r string) *nats.Msg {
				data, _ := json.Marshal(CallResult{OK: false, Error: "FORGED ERROR", Node: "n1"})
				return &nats.Msg{Subject: r, Data: data}
			}, false, "the tool refused: hello"},
		{"a forged success sent before a real error", "fail",
			func(r string) *nats.Msg { return &nats.Msg{Subject: r, Data: forgedResult("FORGED")} }, false, "the tool refused: hello"},
		{"a forged 'nobody is subscribed' status", "say",
			func(r string) *nats.Msg { return &nats.Msg{Subject: r, Header: nats.Header{"Status": []string{"503"}}} }, true, "said:hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := "node.n1.tools.echo." + tc.method
			seen := lab.spyReply(subject)
			before := lab.victim.ReplyRejections()
			wire := &tap{}
			type outcome struct {
				res *CallResult
				err error
			}
			done := make(chan outcome, 1)
			go func() {
				res, err := lab.victim.Call("echo", tc.method, map[string]interface{}{"text": "hello"}, 5*time.Second, "n1")
				done <- outcome{res, err}
			}()
			var reply string
			select {
			case reply = <-seen:
			case <-time.After(3 * time.Second):
				t.Fatal("the request was never published")
			}
			wire.on(t, lab.attacker, reply)
			if err := lab.attacker.PublishMsg(tc.forge(reply)); err != nil {
				t.Fatal(err)
			}
			_ = lab.attacker.Flush()
			out := <-done
			if out.err != nil {
				t.Fatalf("the call failed: %v", out.err)
			}
			// The caller got the real answer, whole.
			if out.res.OK != tc.wantOK || (tc.wantOK && out.res.Result != tc.want) || (!tc.wantOK && out.res.Error != tc.want) {
				t.Fatalf("the caller received %+v, want ok=%v %q", out.res, tc.wantOK, tc.want)
			}
			// The forged message was on the wire, ahead of the real one.
			time.Sleep(50 * time.Millisecond)
			if got := wire.bodies(); len(got) < 2 {
				t.Fatalf("the reply subject carried %d message(s), want the forged one and the real one: %q", len(got), got)
			}
			// And it was counted. The forged status is counted like anything
			// else that lacks the token.
			if rejected := lab.victim.ReplyRejections() - before; rejected != 1 {
				t.Fatalf("rejections = %d, want 1", rejected)
			}
		})
	}
}

// The reflection that matters most: while the victim's call is in flight, the
// adversary asks the SAME honest node a question of its own and names the
// victim's reply subject. The honest node publishes real, well-formed answers
// there. They carry the adversary's token or none, never the victim's.
func TestRequireReplyTokenRejectsAnAnswerReflectedThroughTheRightNode(t *testing.T) {
	lab := newTokenLab(t)
	lab.slowEcho(400 * time.Millisecond)
	seen := lab.spyReply("node.n1.tools.echo.say")
	before := lab.victim.ReplyRejections()
	done := make(chan *CallResult, 1)
	go func() {
		res, err := lab.victim.Call("echo", "say", map[string]interface{}{"text": "victim's question"}, 5*time.Second, "n1")
		if err != nil {
			t.Errorf("the call failed: %v", err)
		}
		done <- res
	}()
	reply := <-seen
	wire := &tap{}
	wire.on(t, lab.attacker, reply)
	// With a token of its own, and with none: both are reflected.
	for _, hdr := range []nats.Header{{replytoken.Header: []string{strings.Repeat("cd", replytoken.Size)}}, nil} {
		data, _ := json.Marshal(CallRequest{Params: map[string]interface{}{"text": "attacker's question"}})
		if err := lab.attacker.PublishMsg(&nats.Msg{Subject: "node.n1.tools.echo.quick", Reply: reply, Data: data, Header: hdr}); err != nil {
			t.Fatal(err)
		}
	}
	_ = lab.attacker.Flush()
	res := <-done
	if res == nil || !res.OK || res.Result != "said:victim's question" {
		t.Fatalf("the caller received %+v, want the answer to its own question", res)
	}
	// Both reflected answers were published by the honest node to the
	// victim's reply subject, and both arrived before the real answer. (The
	// tap is a second subscriber and may see the last message a moment after
	// the caller does.)
	time.Sleep(100 * time.Millisecond)
	bodies := wire.bodies()
	if len(bodies) != 3 || !strings.Contains(bodies[0], "attacker's question") || !strings.Contains(bodies[1], "attacker's question") || !strings.Contains(bodies[2], "victim's question") {
		t.Fatalf("the reply subject carried %q, want two reflected answers and then the real one", bodies)
	}
	if n := lab.victim.ReplyRejections() - before; n != 2 {
		t.Fatalf("rejections = %d, want the 2 reflected answers", n)
	}
}

// A valid token of ANOTHER call by the same requester is not this call's.
func TestRequireReplyTokenIsPerCall(t *testing.T) {
	lab := newTokenLab(t)
	lab.slowEcho(250 * time.Millisecond)
	subject := "node.n1.tools.echo.say"
	// Capture the token of a first call, as a party that received it could.
	tokens := make(chan string, 2)
	replies := make(chan string, 2)
	if _, err := lab.attacker.Subscribe(subject, func(m *nats.Msg) {
		tokens <- replytoken.Of(m)
		replies <- m.Reply
	}); err != nil {
		t.Fatal(err)
	}
	_ = lab.attacker.Flush()
	if _, err := lab.victim.Call("echo", "say", map[string]interface{}{"text": "first"}, 5*time.Second, "n1"); err != nil {
		t.Fatal(err)
	}
	first := <-tokens
	<-replies
	if len(first) != 2*replytoken.Size {
		t.Fatalf("the request carried token %q, want %d hex characters", first, 2*replytoken.Size)
	}
	before := lab.victim.ReplyRejections()
	done := make(chan *CallResult, 1)
	go func() {
		res, _ := lab.victim.Call("echo", "say", map[string]interface{}{"text": "second"}, 5*time.Second, "n1")
		done <- res
	}()
	second, reply := <-tokens, <-replies
	if second == first {
		t.Fatal("two calls carried the same token")
	}
	if err := lab.attacker.PublishMsg(&nats.Msg{Subject: reply, Data: forgedResult("REPLAYED"), Header: nats.Header{replytoken.Header: []string{first}}}); err != nil {
		t.Fatal(err)
	}
	_ = lab.attacker.Flush()
	if res := <-done; res == nil || res.Result != "said:second" {
		t.Fatalf("the caller received %+v", res)
	}
	if n := lab.victim.ReplyRejections() - before; n != 1 {
		t.Fatalf("rejections = %d, want 1", n)
	}
}

// A responder that does not echo the token: every answer is rejected, the
// call ends at its deadline, and the error says what was seen without
// claiming why.
func TestRequireReplyTokenReportsRejectedAnswersAtTheDeadline(t *testing.T) {
	lab := newTokenLab(t)
	if _, err := lab.attacker.Subscribe("node.old.tools.echo.say", func(m *nats.Msg) {
		_ = m.Respond(forgedResult("from a node that echoes nothing"))
	}); err != nil {
		t.Fatal(err)
	}
	_ = lab.attacker.Flush()
	res, err := lab.victim.Call("echo", "say", map[string]interface{}{"text": "x"}, 400*time.Millisecond, "old")
	if err == nil {
		t.Fatalf("a tokenless answer was accepted: %+v", res)
	}
	re, ok := replytoken.AsRejected(err)
	if !ok || re.Rejected != 1 || !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("err = %v, want a timeout that reports one rejected message", err)
	}
	if strings.Contains(err.Error(), "old") && strings.Contains(strings.ToLower(err.Error()), "too old") {
		t.Fatalf("the error claims a cause: %v", err)
	}
	// With nobody subscribed at all, the server says so, but its status
	// cannot carry the token and is not an answer: the call runs to its
	// deadline and reports the status among the rejected.
	start := time.Now()
	_, err = lab.victim.Call("echo", "say", map[string]interface{}{"text": "x"}, 600*time.Millisecond, "nobody")
	re, ok = replytoken.AsRejected(err)
	if !ok || re.Rejected != 1 || !errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("err = %v, want a timeout reporting the rejected status", err)
	}
	if waited := time.Since(start); waited < 500*time.Millisecond {
		t.Fatalf("an unserved verified call ended after %v, before its deadline", waited)
	}
	// The context form reports the context's own error.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := lab.victim.CallWithContext(ctx, "echo", "say", nil, "nobody"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context call to an unserved subject: %v", err)
	}
}

// Only a message carrying the token answers a verified call. A forged
// "nobody is subscribed" status must not end it, however long the real answer
// then takes: here it arrives well over a second later.
func TestRequireReplyTokenWaitsPastAForgedNoRespondersStatus(t *testing.T) {
	lab := newTokenLab(t)
	lab.slowEcho(1500 * time.Millisecond)
	status := func(reply string) {
		if err := lab.attacker.PublishMsg(&nats.Msg{Subject: reply, Header: nats.Header{"Status": []string{"503"}}}); err != nil {
			t.Fatal(err)
		}
		_ = lab.attacker.Flush()
	}
	t.Run("ordinary call", func(t *testing.T) {
		seen := lab.spyReply("node.n1.tools.echo.say")
		before := lab.victim.ReplyRejections()
		done := make(chan *CallResult, 1)
		start := time.Now()
		go func() {
			res, err := lab.victim.Call("echo", "say", map[string]interface{}{"text": "slow"}, 10*time.Second, "n1")
			if err != nil {
				t.Errorf("the call failed: %v", err)
			}
			done <- res
		}()
		status(<-seen)
		res := <-done
		if res == nil || res.Result != "said:slow" {
			t.Fatalf("the caller received %+v", res)
		}
		if waited := time.Since(start); waited < 1400*time.Millisecond {
			t.Fatalf("the call returned after %v; the real answer takes 1.5s", waited)
		}
		if n := lab.victim.ReplyRejections() - before; n != 1 {
			t.Fatalf("rejections = %d, want the forged status", n)
		}
	})
	t.Run("stream whose first frame is slow", func(t *testing.T) {
		nc := lab.server.Conn()
		if _, err := nc.Subscribe(NodeStreamSubject("slow", "feed", "watch"), func(req *nats.Msg) {
			go func() {
				time.Sleep(1500 * time.Millisecond)
				for _, f := range []StreamFrame{{Chunk: "first"}, {Result: "real", Done: true}} {
					data, _ := json.Marshal(f)
					_ = replytoken.Publish(nc, req, data)
				}
			}()
		}); err != nil {
			t.Fatal(err)
		}
		_ = nc.Flush()
		seen := lab.spyReply(NodeStreamSubject("slow", "feed", "watch"))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		before := lab.victim.ReplyRejections()
		frames, err := lab.victim.Stream(ctx, "feed", "watch", nil, "slow")
		if err != nil {
			t.Fatal(err)
		}
		status(<-seen)
		var got []StreamFrame
		for f := range frames {
			got = append(got, f)
		}
		if len(got) != 2 || got[0].Chunk != "first" || got[1].Result != "real" || got[1].Error != "" {
			t.Fatalf("a forged status changed a served stream: %+v", got)
		}
		if n := lab.victim.ReplyRejections() - before; n != 1 {
			t.Fatalf("rejections = %d, want the forged status", n)
		}
	})
}

// Without RequireReplyToken a connection behaves as it always has, and a
// node still echoes the token for those who check.
func TestWithoutRequireReplyTokenNothingChangesForTheCaller(t *testing.T) {
	lab := newTokenLab(t)
	lab.slowEcho(0)
	plain, err := New(Config{NATSUrl: lab.url, NodeName: "plain", Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	res, err := plain.Call("echo", "say", map[string]interface{}{"text": "hi"}, 2*time.Second, "n1")
	if err != nil || res.Result != "said:hi" {
		t.Fatalf("plain call: %+v %v", res, err)
	}
	// Unpinned (queue) subject through the micro endpoint echoes too.
	res, err = lab.victim.Call("echo", "say", map[string]interface{}{"text": "via the queue subject"}, 2*time.Second)
	if err != nil || res.Result != "said:via the queue subject" {
		t.Fatalf("micro endpoint under RequireReplyToken: %+v %v", res, err)
	}
	// Context form, and no responders stays immediate for a plain connection.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if res, err := lab.victim.CallWithContext(ctx, "echo", "say", map[string]interface{}{"text": "ctx"}, "n1"); err != nil || res.Result != "said:ctx" {
		t.Fatalf("context call: %+v %v", res, err)
	}
	start := time.Now()
	if _, err := plain.Call("echo", "say", nil, 3*time.Second, "nobody"); !errors.Is(err, nats.ErrNoResponders) || time.Since(start) > time.Second {
		t.Fatalf("plain no-responders: %v after %v", err, time.Since(start))
	}
}

// streamServer answers a stream request the way a node does: one message per
// frame to the request's reply subject, each carrying the request's token,
// with a pause between frames so an adversary can inject between them.
func (lab *tokenLab) streamServer(node string, frames int, gap time.Duration) {
	lab.t.Helper()
	nc := lab.server.Conn()
	if _, err := nc.Subscribe(NodeStreamSubject(node, "feed", "watch"), func(req *nats.Msg) {
		go func() {
			for i := 0; i < frames; i++ {
				data, _ := json.Marshal(StreamFrame{Chunk: map[string]interface{}{"n": i}, Node: node})
				_ = replytoken.Publish(nc, req, data)
				time.Sleep(gap)
			}
			data, _ := json.Marshal(StreamFrame{Result: "complete", Done: true, Node: node})
			_ = replytoken.Publish(nc, req, data)
		}()
	}); err != nil {
		lab.t.Fatal(err)
	}
	_ = nc.Flush()
}

func TestRequireReplyTokenDropsFramesInjectedIntoAStream(t *testing.T) {
	lab := newTokenLab(t)
	lab.streamServer("n1", 5, 80*time.Millisecond)
	subject := NodeStreamSubject("n1", "feed", "watch")
	seen := lab.spyReply(subject)
	before := lab.victim.ReplyRejections()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	frames, err := lab.victim.Stream(ctx, "feed", "watch", nil, "n1")
	if err != nil {
		t.Fatal(err)
	}
	reply := <-seen
	wire := &tap{}
	wire.on(t, lab.attacker, reply)
	// While the stream runs, inject: a frame between real frames, a forged
	// terminal frame that would end the stream early, an error frame, a
	// frame with a made-up token, and a forged "nobody is subscribed".
	inject := func(msg *nats.Msg) {
		msg.Subject = reply
		if err := lab.attacker.PublishMsg(msg); err != nil {
			t.Error(err)
		}
		_ = lab.attacker.Flush()
	}
	frame := func(f StreamFrame) []byte { data, _ := json.Marshal(f); return data }
	go func() {
		time.Sleep(120 * time.Millisecond)
		inject(&nats.Msg{Data: frame(StreamFrame{Chunk: map[string]interface{}{"n": "FORGED"}})})
		inject(&nats.Msg{Data: frame(StreamFrame{Result: "FORGED END", Done: true})})
		inject(&nats.Msg{Data: frame(StreamFrame{Error: "FORGED ERROR", Done: true})})
		inject(&nats.Msg{Data: frame(StreamFrame{Chunk: map[string]interface{}{"n": "FORGED"}}), Header: nats.Header{replytoken.Header: []string{strings.Repeat("ef", replytoken.Size)}}})
		inject(&nats.Msg{Header: nats.Header{"Status": []string{"503"}}})
		inject(&nats.Msg{Data: []byte("not json at all")})
	}()
	var got []StreamFrame
	for f := range frames {
		got = append(got, f)
	}
	// Exactly the real stream: five chunks in order, then the real terminal.
	if len(got) != 6 {
		t.Fatalf("the caller received %d frames, want 5 chunks and the terminal: %+v", len(got), got)
	}
	for i := 0; i < 5; i++ {
		chunk, _ := got[i].Chunk.(map[string]interface{})
		if got[i].Done || got[i].Error != "" || chunk["n"] != float64(i) {
			t.Fatalf("frame %d = %+v", i, got[i])
		}
	}
	if !got[5].Done || got[5].Result != "complete" || got[5].Error != "" {
		t.Fatalf("terminal = %+v", got[5])
	}
	// All six injected messages were on the wire, and all six were rejected.
	time.Sleep(100 * time.Millisecond)
	if n := len(wire.bodies()); n < 6+5 {
		t.Fatalf("the reply subject carried %d messages, want the 6 injected and the real frames", n)
	}
	if n := lab.victim.ReplyRejections() - before; n != 6 {
		t.Fatalf("rejections = %d, want 6", n)
	}
}

// A verified stream nobody serves is not ended by the server's status. It
// runs to the caller's deadline and then says what arrived and was rejected.
func TestRequireReplyTokenUnservedStreamRunsToItsDeadline(t *testing.T) {
	lab := newTokenLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	frames, err := lab.victim.Stream(ctx, "feed", "watch", nil, "nobody")
	if err != nil {
		t.Fatal(err)
	}
	var got []StreamFrame
	for f := range frames {
		got = append(got, f)
	}
	if waited := time.Since(start); waited < 400*time.Millisecond {
		t.Fatalf("an unserved verified stream ended after %v, before its deadline", waited)
	}
	if len(got) != 1 || !got[0].Done || !strings.Contains(got[0].Error, "1 message(s)") || !strings.Contains(got[0].Error, "rejected") {
		t.Fatalf("an unserved stream delivered %+v, want one terminal frame reporting the rejected status", got)
	}
	// A connection that does not require the token is told at once.
	plain, err := New(Config{NATSUrl: lab.url, NodeName: "plain", Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	start = time.Now()
	frames, err = plain.Stream(ctx2, "feed", "watch", nil, "nobody")
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	for f := range frames {
		got = append(got, f)
	}
	if len(got) != 1 || !got[0].Done || !strings.Contains(got[0].Error, "no responders") || time.Since(start) > time.Second {
		t.Fatalf("a plain unserved stream delivered %+v after %v", got, time.Since(start))
	}
}

// Every responder a node runs echoes the token. A connection that requires it
// gets an answer from each of them; one that forgot would time out here.
func TestEveryNodeResponderEchoesTheReplyToken(t *testing.T) {
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	defer ns.Shutdown()
	server, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "n1", Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	victim, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "victim", RequireReplyToken: true, Logging: LoggingConfig{Quiet: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer victim.Close()

	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	must("tool", server.RegisterTool("echo", "1.0.0", "echo", []string{"say"},
		func(_ CallRequest, _ string, p map[string]interface{}) (interface{}, error) { return p["text"], nil }))
	must("uninstall", server.SubscribeUninstall(func(string, bool) error { return nil }))
	must("update", server.SubscribeUpdate(func(string, bool) (string, string, error) { return "1", "2", nil }))
	must("config", server.SubscribeConfig(func(string, string, map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{"k": "v"}, nil
	}))
	must("release inspect", server.SubscribeReleaseInspect(func(string) (*ReleaseInspectInfo, error) { return &ReleaseInspectInfo{}, nil }))
	must("install", server.SubscribeInstall(func(string) (string, string, error) { return "tool", "1.0.0", nil }))
	must("schema", server.SubscribeToolSchema(func(string) (map[string]interface{}, error) { return map[string]interface{}{"say": 1}, nil }))
	js, err := server.Conn().JetStream()
	must("jetstream", err)
	store, err := filestore.NewStore(js, filestore.Config{})
	must("file store", err)
	must("file handlers", server.SubscribeFileHandlers(store))
	must("flush", server.Conn().Flush())

	const wait = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	raw := func(subject string, body string) {
		t.Helper()
		if _, err := victim.RequestVerified(ctx, subject, []byte(body)); err != nil {
			t.Fatalf("%s: no token-carrying answer: %v", subject, err)
		}
	}
	// Tool calls: node-pinned, queue (micro), and each one's refusal of a
	// request that does not parse.
	if res, err := victim.Call("echo", "say", map[string]interface{}{"text": "a"}, wait, "n1"); err != nil || res.Result != "a" {
		t.Fatalf("node-pinned call: %+v %v", res, err)
	}
	if res, err := victim.Call("echo", "say", map[string]interface{}{"text": "b"}, wait); err != nil || res.Result != "b" {
		t.Fatalf("queue call: %+v %v", res, err)
	}
	raw("node.n1.tools.echo.say", "{not json")
	raw("tools.echo.say", "{not json")
	// Management requests, the answer and the refusal of an unparseable one.
	if _, err := victim.RequestUninstall("n1", "echo", false, wait); err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if _, err := victim.RequestUpdate("n1", "echo", false, wait); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := victim.RequestConfig("n1", "echo", "get", nil, wait); err != nil {
		t.Fatalf("config: %v", err)
	}
	if _, err := victim.RequestReleaseInspect("n1", "echo", wait); err != nil {
		t.Fatalf("release inspect: %v", err)
	}
	if _, err := victim.RequestInstall("n1", "some-source", wait); err != nil {
		t.Fatalf("install: %v", err)
	}
	if _, err := victim.GetToolSchema("n1", "echo", wait); err != nil {
		t.Fatalf("schema: %v", err)
	}
	// File handlers: every verb answers, including for a key that is absent.
	for _, subject := range []string{"files.get", "files.head", "files.list", "files.delete", "files.put"} {
		raw(subject, `{"key":"absent/key.txt"}`)
	}
	if n := victim.ReplyRejections(); n != 0 {
		t.Fatalf("%d answer(s) from a node's own responders were rejected", n)
	}
}
