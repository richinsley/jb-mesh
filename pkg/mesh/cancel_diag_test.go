//go:build integration

package mesh

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestPhase1_DiagTwoHealthCallsNoCancel: baseline — two CallWithContext calls
// in a row, no cancel anywhere. If this fails, the bug is in CallWithContext;
// if this passes, the cancel scenario specifically breaks the next call.
func TestPhase1_DiagTwoHealthCallsNoCancel(t *testing.T) {
	natsURL := os.Getenv("PHASE1_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:14222"
	}
	m, err := New(Config{NATSUrl: natsURL, NodeName: "phase1-diag"})
	if err != nil {
		t.Skipf("dev mesh not reachable: %v", err)
	}
	defer m.Close()

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := m.CallWithContext(ctx, "sleeptool", "health", nil)
		cancel()
		if err != nil {
			t.Fatalf("health call %d failed: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("health call %d not-ok: %s", i, res.Error)
		}
		t.Logf("health call %d ok", i)
	}
}
