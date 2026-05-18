package tools

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// RecoveryConfig controls crash recovery behavior
type RecoveryConfig struct {
	InitialBackoff time.Duration // First retry delay (default 1s)
	MaxBackoff     time.Duration // Maximum retry delay (default 60s)
	MaxRetries     int           // Consecutive failures before giving up (default 5, 0 = unlimited)
}

// DefaultRecoveryConfig returns sensible defaults per DESIGN.md §3.6
func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     60 * time.Second,
		MaxRetries:     5,
	}
}

// toolRecoveryState tracks per-tool recovery attempts
type toolRecoveryState struct {
	failures    int
	lastBackoff time.Duration
}

// CrashMonitor watches persistent tools and auto-restarts them on failure.
type CrashMonitor struct {
	executor *Executor
	config   RecoveryConfig
	states   map[string]*toolRecoveryState
	cancels  map[string]context.CancelFunc
	onCrash  func(toolName string, err error, attempt int) // optional callback
	onGiveUp func(toolName string, failures int)           // optional callback
	mu       sync.Mutex
}

// NewCrashMonitor creates a crash monitor for the given executor.
func NewCrashMonitor(executor *Executor, cfg RecoveryConfig) *CrashMonitor {
	return &CrashMonitor{
		executor: executor,
		config:   cfg,
		states:   make(map[string]*toolRecoveryState),
		cancels:  make(map[string]context.CancelFunc),
	}
}

// OnCrash sets a callback invoked when a crash is detected (before restart attempt).
func (cm *CrashMonitor) OnCrash(fn func(toolName string, err error, attempt int)) {
	cm.onCrash = fn
}

// OnGiveUp sets a callback invoked when max retries are exhausted.
func (cm *CrashMonitor) OnGiveUp(fn func(toolName string, failures int)) {
	cm.onGiveUp = fn
}

// Watch begins monitoring a persistent tool. The check function is called
// periodically to determine if the tool's process is still alive.
// If it returns an error, the tool is considered crashed.
func (cm *CrashMonitor) Watch(toolName string, interval time.Duration, isAlive func() error) {
	cm.mu.Lock()
	// Cancel existing monitor if any
	if cancel, ok := cm.cancels[toolName]; ok {
		cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cm.cancels[toolName] = cancel
	cm.states[toolName] = &toolRecoveryState{
		lastBackoff: cm.config.InitialBackoff,
	}
	cm.mu.Unlock()

	go cm.monitorLoop(ctx, toolName, interval, isAlive)
}

// Unwatch stops monitoring a tool.
func (cm *CrashMonitor) Unwatch(toolName string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cancel, ok := cm.cancels[toolName]; ok {
		cancel()
		delete(cm.cancels, toolName)
		delete(cm.states, toolName)
	}
}

// Reset clears the failure count for a tool (e.g., after a successful health check).
func (cm *CrashMonitor) Reset(toolName string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if state, ok := cm.states[toolName]; ok {
		state.failures = 0
		state.lastBackoff = cm.config.InitialBackoff
	}
}

// Failures returns the current consecutive failure count for a tool.
func (cm *CrashMonitor) Failures(toolName string) int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if state, ok := cm.states[toolName]; ok {
		return state.failures
	}
	return 0
}

// Close stops all monitors.
func (cm *CrashMonitor) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for name, cancel := range cm.cancels {
		cancel()
		delete(cm.cancels, name)
	}
	cm.states = make(map[string]*toolRecoveryState)
}

// monitorLoop is the main monitoring goroutine for a single tool.
func (cm *CrashMonitor) monitorLoop(ctx context.Context, toolName string, interval time.Duration, isAlive func() error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := isAlive()
			if err == nil {
				// Tool is alive — reset failure tracking
				cm.Reset(toolName)
				continue
			}

			// Tool crashed
			cm.mu.Lock()
			state, ok := cm.states[toolName]
			if !ok {
				cm.mu.Unlock()
				return // unwatched during iteration
			}
			state.failures++
			failures := state.failures
			backoff := state.lastBackoff
			cm.mu.Unlock()

			log.Printf("[recovery] %s crashed (attempt %d): %v", toolName, failures, err)

			if cm.onCrash != nil {
				cm.onCrash(toolName, err, failures)
			}

			// Check if we've exceeded max retries
			if cm.config.MaxRetries > 0 && failures >= cm.config.MaxRetries {
				log.Printf("[recovery] %s: giving up after %d failures", toolName, failures)
				if cm.onGiveUp != nil {
					cm.onGiveUp(toolName, failures)
				}

				// Mark tool as stopped
				if tool, ok := cm.executor.manager.Get(toolName); ok {
					tool.Status = "crashed"
					tool.HealthStatus = "unhealthy"
				}
				return
			}

			// Wait with backoff before restart
			log.Printf("[recovery] %s: restarting in %v", toolName, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			// Attempt restart
			restartErr := cm.executor.Start(toolName)
			if restartErr != nil {
				log.Printf("[recovery] %s: restart failed: %v", toolName, restartErr)
				// Update backoff (exponential, capped)
				cm.mu.Lock()
				if s, ok := cm.states[toolName]; ok {
					s.lastBackoff = min(s.lastBackoff*2, cm.config.MaxBackoff)
				}
				cm.mu.Unlock()
			} else {
				log.Printf("[recovery] %s: restarted successfully", toolName)
				// Don't fully reset — keep failure count so we still give up
				// if it keeps crashing. Reset backoff though.
				cm.mu.Lock()
				if s, ok := cm.states[toolName]; ok {
					s.lastBackoff = cm.config.InitialBackoff
				}
				cm.mu.Unlock()
			}
		}
	}
}

// min returns the smaller of two durations.
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// IsProcessAlive returns a check function for REPL-based tools.
// It attempts a simple ping via the REPL to verify the process is responsive.
func IsProcessAlive(executor *Executor, toolName string) func() error {
	return func() error {
		executor.mu.RLock()
		repl, hasRepl := executor.repls[toolName]
		queue, hasQueue := executor.queues[toolName]
		executor.mu.RUnlock()

		if hasRepl && repl != nil {
			// Try a simple expression
			result, err := repl.Execute("1+1", true)
			if err != nil {
				return fmt.Errorf("REPL unresponsive: %w", err)
			}
			if result == "" {
				return fmt.Errorf("REPL returned empty response")
			}
			return nil
		}

		if hasQueue && queue != nil {
			// For queue processes, check if the process is still running
			// by attempting a lightweight call
			_, err := queue.Call("__ping__", 5, map[string]interface{}{})
			if err != nil {
				// __ping__ might not exist, but if the process is alive
				// we'll get a method-not-found error, not a connection error.
				// Connection errors indicate a dead process.
				errStr := err.Error()
				if contains(errStr, "method") || contains(errStr, "not found") || contains(errStr, "unknown") {
					return nil // Process alive, method just doesn't exist
				}
				return fmt.Errorf("queue process unresponsive: %w", err)
			}
			return nil
		}

		return fmt.Errorf("tool %s has no active process", toolName)
	}
}

// contains is a simple string containment check
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
