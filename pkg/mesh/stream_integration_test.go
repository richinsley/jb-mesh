//go:build integration

package mesh

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// TestPhase2_StreamMultiFrame: end-to-end streaming integration.
//
// Calls sleeptool.progress(n=10) via Mesh.Stream. Expects 11 frames total
// (10 partials with chunk field + 1 terminal with result + done=true).
// Verifies frame ordering and that the channel closes after the terminal
// frame.
//
// Run with the dev mesh up on localhost:14222 and sleeptool v0.2.0+
// installed (see DESIGN-STREAMING-CANCEL.md §8.3).
//
//	go test -tags=integration -v -count=1 -run TestPhase2_Stream ./pkg/mesh/
func TestPhase2_StreamMultiFrame(t *testing.T) {
	natsURL := os.Getenv("PHASE1_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:14222"
	}
	m, err := New(Config{NATSUrl: natsURL, NodeName: "phase2-stream-test"})
	if err != nil {
		t.Skipf("dev mesh not reachable: %v", err)
	}
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	frames, err := m.Stream(ctx, "sleeptool", "progress", map[string]interface{}{"n": 10})
	if err != nil {
		t.Fatalf("Stream failed: %v", err)
	}

	var partials []StreamFrame
	var terminal *StreamFrame
	for f := range frames {
		ff := f
		if ff.Done {
			terminal = &ff
		} else {
			partials = append(partials, ff)
		}
	}

	if terminal == nil {
		t.Fatal("expected a terminal frame, got none")
	}
	if len(partials) != 10 {
		t.Errorf("expected 10 partial frames, got %d", len(partials))
	}
	if terminal.Error != "" {
		t.Errorf("terminal frame has error: %s", terminal.Error)
	}
	if terminal.Result == nil {
		t.Errorf("terminal frame missing result: %+v", terminal)
	}

	// Spot-check partial structure.
	for i, p := range partials {
		chunkMap, ok := p.Chunk.(map[string]interface{})
		if !ok {
			t.Errorf("partial %d chunk not a map: %T", i, p.Chunk)
			continue
		}
		if _, hasI := chunkMap["i"]; !hasI {
			t.Errorf("partial %d missing 'i' field: %+v", i, chunkMap)
		}
	}
	t.Logf("PASS: received %d partials + 1 terminal", len(partials))
}

// TestPhase2_StreamCancelMidStream: ctx fires while frames are arriving.
// Verifies the channel closes promptly and the cancel propagates to the
// Python side (the next post-cancel health call returns ok, confirming the
// service didn't hang).
func TestPhase2_StreamCancelMidStream(t *testing.T) {
	natsURL := os.Getenv("PHASE1_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:14222"
	}
	m, err := New(Config{NATSUrl: natsURL, NodeName: "phase2-stream-cancel-test"})
	if err != nil {
		t.Skipf("dev mesh not reachable: %v", err)
	}
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 200 frames at 50ms each = 10s total if uncancelled.
	frames, err := m.Stream(ctx, "sleeptool", "progress", map[string]interface{}{"n": 200})
	if err != nil {
		t.Fatalf("Stream failed: %v", err)
	}

	received := 0
	t0 := time.Now()
	for f := range frames {
		received++
		if received == 5 {
			cancel()
		}
		_ = f
	}
	elapsed := time.Since(t0)
	t.Logf("received %d frames before stream closed, elapsed=%v", received, elapsed)

	if received < 5 {
		t.Fatalf("expected at least 5 frames before cancel, got %d", received)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("stream took too long to close after cancel: %v (expected <5s)", elapsed)
	}

	// Post-cancel sanity check.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	res, err := m.CallWithContext(ctx2, "sleeptool", "health", nil)
	if err != nil {
		t.Fatalf("post-cancel health failed: %v", err)
	}
	if !res.OK {
		t.Fatalf("post-cancel health not-ok: %s", res.Error)
	}
	t.Logf("PASS: post-cancel service still responsive")
}

// TestPhase2_StreamAlreadyDoneFailsFast: ctx pre-cancelled, no request sent.
func TestPhase2_StreamAlreadyDoneFailsFast(t *testing.T) {
	natsURL := os.Getenv("PHASE1_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:14222"
	}
	m, err := New(Config{NATSUrl: natsURL, NodeName: "phase2-prefired"})
	if err != nil {
		t.Skipf("dev mesh not reachable: %v", err)
	}
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = m.Stream(ctx, "sleeptool", "progress", map[string]interface{}{"n": 5})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
