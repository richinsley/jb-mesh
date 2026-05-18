package mesh

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestCallWithContext_Success: a normal call with a non-fired ctx returns
// the result like Call() would.
func TestCallWithContext_Success(t *testing.T) {
	ns := startTestNATS(t)
	server, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "server"})
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	defer server.Close()

	err = server.RegisterTool("calc", "1.0.0", "calc", []string{"add"},
		func(req CallRequest, method string, params map[string]interface{}) (interface{}, error) {
			a := params["a"].(float64)
			b := params["b"].(float64)
			return map[string]interface{}{"sum": a + b}, nil
		},
	)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	client, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "client"})
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := client.CallWithContext(ctx, "calc", "add", map[string]interface{}{"a": 2.0, "b": 3.0})
	if err != nil {
		t.Fatalf("CallWithContext: %v", err)
	}
	if !result.OK {
		t.Fatalf("expected OK, got error: %s", result.Error)
	}
	m := result.Result.(map[string]interface{})
	if m["sum"] != 3.0+2.0 {
		t.Errorf("expected sum=5, got %v", m["sum"])
	}
}

// TestCallWithContext_AlreadyDoneFailsFast: ctx already cancelled — no NATS
// request is sent, error is returned immediately.
func TestCallWithContext_AlreadyDoneFailsFast(t *testing.T) {
	ns := startTestNATS(t)
	client, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "client"})
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.CallWithContext(ctx, "nonexistent", "method", nil)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestCallWithContext_CancelPublishesToCancelSubject: when ctx fires mid-call,
// the caller publishes to `cancel.<call_id>`. A subscribing server can act on
// the cancel (the full mesh layer is exercised in TestCallWithContext_EndToEnd
// below; this test just verifies the cancel publish happens with the right
// call_id).
func TestCallWithContext_CancelPublishesToCancelSubject(t *testing.T) {
	ns := startTestNATS(t)

	// Server registers a slow handler that captures the call_id from the request.
	server, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "server"})
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	defer server.Close()

	var (
		mu          sync.Mutex
		capturedID  string
		handlerDone = make(chan struct{})
	)

	err = server.RegisterTool("slow", "1.0.0", "slow", []string{"work"},
		func(req CallRequest, method string, params map[string]interface{}) (interface{}, error) {
			mu.Lock()
			capturedID = req.CallID
			mu.Unlock()
			// Block until either we receive a cancel publish or 5s elapses.
			cancelCh := make(chan struct{}, 1)
			sub, _ := server.nc.Subscribe(CancelSubject(req.CallID), func(_ *nats.Msg) {
				select {
				case cancelCh <- struct{}{}:
				default:
				}
			})
			defer sub.Unsubscribe()
			select {
			case <-cancelCh:
				close(handlerDone)
				return nil, nil // server-side cancel observed; respond with empty
			case <-time.After(5 * time.Second):
				close(handlerDone)
				return map[string]interface{}{"timed_out": true}, nil
			}
		},
	)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	client, err := New(Config{NATSUrl: ns.ClientURL(), NodeName: "client"})
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type res struct {
		val *CallResult
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		v, e := client.CallWithContext(ctx, "slow", "work", nil)
		resCh <- res{v, e}
	}()

	// Give the server time to receive the request and subscribe to its cancel subject.
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Caller should return ctx.Err() promptly.
	select {
	case r := <-resCh:
		if r.err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v (val=%+v)", r.err, r.val)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CallWithContext didn't return within 2s of ctx cancellation")
	}

	// Server-side: the handler's cancel subscription should have fired.
	select {
	case <-handlerDone:
		// good - handler observed cancel and returned
	case <-time.After(2 * time.Second):
		t.Fatal("server handler didn't observe cancel within 2s")
	}

	mu.Lock()
	id := capturedID
	mu.Unlock()
	if id == "" {
		t.Error("server didn't see a CallID on the request")
	}
}
