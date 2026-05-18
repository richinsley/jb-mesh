//go:build integration

package mesh

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// TestPhase1_CancelEndToEnd is the integration test for Phase 1 of the
// streaming+cancellation design. It requires a running dev mesh with the
// sleeptool service installed.
//
// Run with:
//
//	# Terminal 1: start dev mesh + install sleeptool (see DESIGN-STREAMING-CANCEL.md §8.3)
//	# Terminal 2:
//	go test -tags=integration -v -run TestPhase1_CancelEndToEnd ./pkg/mesh/
//
// Optional env: PHASE1_NATS_URL (default: nats://localhost:14222).
//
// Assertions:
//   - sleeptool.sleep(30) with a 1s ctx deadline returns within ~5s
//   - The returned error is context.DeadlineExceeded
//   - A health call afterward still works (the service wasn't torn down)
func TestPhase1_CancelEndToEnd(t *testing.T) {
	natsURL := os.Getenv("PHASE1_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:14222"
	}

	m, err := New(Config{
		NATSUrl:  natsURL,
		NodeName: "phase1-cancel-test",
	})
	if err != nil {
		t.Skipf("dev mesh not reachable at %s (skipping): %v", natsURL, err)
	}
	defer m.Close()

	// Sanity: pre-cancel health call should succeed quickly.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := m.CallWithContext(ctx, "sleeptool", "health", nil)
		if err != nil {
			t.Fatalf("pre-cancel health failed: %v", err)
		}
		if !res.OK {
			t.Fatalf("pre-cancel health returned not-ok: %s", res.Error)
		}
	}

	// The actual test: long sleep with short ctx deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	t0 := time.Now()
	_, err = m.CallWithContext(ctx, "sleeptool", "sleep", map[string]interface{}{"seconds": 30.0})
	elapsed := time.Since(t0)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v (elapsed %v)", err, elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("cancellation took too long (%v) — Python likely didn't observe cancel", elapsed)
	}
	t.Logf("PASS: cancel propagated in %v (ctx deadline was 1s; sleep was 30s)", elapsed)

	// Give the server a moment to fully drain the cancelled call before next request.
	time.Sleep(500 * time.Millisecond)

	// Diagnostic: try non-ctx Mesh.Call on the SAME connection.
	{
		res, err := m.Call("sleeptool", "health", nil, 5*time.Second)
		if err != nil {
			t.Logf("DIAG: post-cancel Mesh.Call (same conn) failed: %v", err)
		} else if !res.OK {
			t.Logf("DIAG: post-cancel Mesh.Call (same conn) not-ok: %s", res.Error)
		} else {
			t.Logf("DIAG: post-cancel Mesh.Call (same conn) works fine")
		}
	}

	// Diagnostic: try CallWithContext on a FRESH Mesh connection.
	{
		m2, err := New(Config{NATSUrl: natsURL, NodeName: "phase1-cancel-test-2"})
		if err != nil {
			t.Fatalf("fresh mesh connect: %v", err)
		}
		defer m2.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := m2.CallWithContext(ctx, "sleeptool", "health", nil)
		if err != nil {
			t.Logf("DIAG: post-cancel CallWithContext (fresh conn) failed: %v", err)
		} else if !res.OK {
			t.Logf("DIAG: post-cancel CallWithContext (fresh conn) not-ok: %s", res.Error)
		} else {
			t.Logf("DIAG: post-cancel CallWithContext (fresh conn) works fine")
		}
	}

	// Sanity: post-cancel health should still work; cancellation didn't kill the service.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := m.CallWithContext(ctx, "sleeptool", "health", nil)
		if err != nil {
			t.Fatalf("post-cancel health (same conn) failed: %v", err)
		}
		if !res.OK {
			t.Fatalf("post-cancel health returned not-ok: %s", res.Error)
		}
		t.Logf("PASS: post-cancel health responded ok")
	}
}
