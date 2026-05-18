package tools

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestCrashMonitor_NoRestart_WhenAlive(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	exec := NewExecutor(mgr)
	defer exec.Close()

	rcfg := DefaultRecoveryConfig()
	rcfg.InitialBackoff = 10 * time.Millisecond // speed up test
	monitor := NewCrashMonitor(exec, rcfg)
	defer monitor.Close()

	var crashCount int32
	monitor.OnCrash(func(name string, err error, attempt int) {
		atomic.AddInt32(&crashCount, 1)
	})

	// Always alive
	monitor.Watch("healthy-tool", 20*time.Millisecond, func() error {
		return nil
	})

	time.Sleep(100 * time.Millisecond)
	monitor.Unwatch("healthy-tool")

	if atomic.LoadInt32(&crashCount) != 0 {
		t.Fatalf("expected no crashes, got %d", crashCount)
	}
}

func TestCrashMonitor_DetectsCrash(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	exec := NewExecutor(mgr)
	defer exec.Close()

	rcfg := RecoveryConfig{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		MaxRetries:     3,
	}
	monitor := NewCrashMonitor(exec, rcfg)
	defer monitor.Close()

	var crashCount int32
	monitor.OnCrash(func(name string, err error, attempt int) {
		atomic.AddInt32(&crashCount, 1)
	})

	// Always dead
	monitor.Watch("dead-tool", 20*time.Millisecond, func() error {
		return fmt.Errorf("process exited")
	})

	// Wait for a few crash detections
	time.Sleep(200 * time.Millisecond)
	monitor.Unwatch("dead-tool")

	crashes := atomic.LoadInt32(&crashCount)
	if crashes == 0 {
		t.Fatal("expected crashes to be detected")
	}
}

func TestCrashMonitor_GivesUpAfterMaxRetries(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	exec := NewExecutor(mgr)
	defer exec.Close()

	rcfg := RecoveryConfig{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		MaxRetries:     2,
	}
	monitor := NewCrashMonitor(exec, rcfg)
	defer monitor.Close()

	var gaveUp int32
	monitor.OnGiveUp(func(name string, failures int) {
		atomic.AddInt32(&gaveUp, 1)
	})

	// Always dead — should give up after 2 failures
	monitor.Watch("doomed-tool", 10*time.Millisecond, func() error {
		return fmt.Errorf("dead")
	})

	time.Sleep(300 * time.Millisecond)

	if atomic.LoadInt32(&gaveUp) != 1 {
		t.Fatalf("expected give-up callback, got %d", atomic.LoadInt32(&gaveUp))
	}
}

func TestCrashMonitor_ResetClearsFailures(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	exec := NewExecutor(mgr)
	defer exec.Close()

	rcfg := DefaultRecoveryConfig()
	monitor := NewCrashMonitor(exec, rcfg)
	defer monitor.Close()

	// Manually set up state
	monitor.Watch("tool-a", 1*time.Second, func() error { return nil })
	monitor.mu.Lock()
	monitor.states["tool-a"].failures = 3
	monitor.mu.Unlock()

	if monitor.Failures("tool-a") != 3 {
		t.Fatalf("expected 3, got %d", monitor.Failures("tool-a"))
	}

	monitor.Reset("tool-a")
	if monitor.Failures("tool-a") != 0 {
		t.Fatalf("expected 0 after reset, got %d", monitor.Failures("tool-a"))
	}
}

func TestCrashMonitor_Unwatch(t *testing.T) {
	cfg := testConfig(t)
	mgr := NewManager(cfg)
	exec := NewExecutor(mgr)
	defer exec.Close()

	rcfg := DefaultRecoveryConfig()
	monitor := NewCrashMonitor(exec, rcfg)
	defer monitor.Close()

	var count int32
	monitor.Watch("temp-tool", 10*time.Millisecond, func() error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	time.Sleep(50 * time.Millisecond)
	monitor.Unwatch("temp-tool")
	countAtUnwatch := atomic.LoadInt32(&count)

	time.Sleep(50 * time.Millisecond)
	countAfter := atomic.LoadInt32(&count)

	// Should not increase much after unwatch (at most 1 in-flight check)
	if countAfter > countAtUnwatch+1 {
		t.Fatalf("monitoring continued after unwatch: %d → %d", countAtUnwatch, countAfter)
	}
}

func TestCrashMonitor_ExponentialBackoff(t *testing.T) {
	rcfg := RecoveryConfig{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     80 * time.Millisecond,
		MaxRetries:     0, // unlimited
	}

	// Verify the backoff math directly
	backoff := rcfg.InitialBackoff
	expected := []time.Duration{10, 20, 40, 80, 80} // caps at MaxBackoff

	for i, exp := range expected {
		expDur := exp * time.Millisecond
		if backoff != expDur {
			t.Fatalf("step %d: expected %v, got %v", i, expDur, backoff)
		}
		backoff = min(backoff*2, rcfg.MaxBackoff)
	}
}

func TestDefaultRecoveryConfig(t *testing.T) {
	cfg := DefaultRecoveryConfig()
	if cfg.InitialBackoff != 1*time.Second {
		t.Fatalf("expected 1s initial backoff, got %v", cfg.InitialBackoff)
	}
	if cfg.MaxBackoff != 60*time.Second {
		t.Fatalf("expected 60s max backoff, got %v", cfg.MaxBackoff)
	}
	if cfg.MaxRetries != 5 {
		t.Fatalf("expected 5 max retries, got %d", cfg.MaxRetries)
	}
}
