package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNew_DefaultLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Output: &buf})

	logger.Info("hello")
	logger.Debug("hidden")

	out := buf.String()
	if !strings.Contains(out, "hello") {
		t.Fatal("expected info message in output")
	}
	if strings.Contains(out, "hidden") {
		t.Fatal("debug should be hidden at default (info) level")
	}
}

func TestNew_DebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Level: "debug", Output: &buf})

	logger.Debug("visible")

	if !strings.Contains(buf.String(), "visible") {
		t.Fatal("debug should be visible at debug level")
	}
}

func TestNew_ErrorLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Level: "error", Output: &buf})

	logger.Info("hidden")
	logger.Warn("also hidden")
	logger.Error("visible")

	out := buf.String()
	if strings.Contains(out, "hidden") {
		t.Fatal("info/warn should be hidden at error level")
	}
	if !strings.Contains(out, "visible") {
		t.Fatal("error should be visible at error level")
	}
}

func TestNew_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Output: &buf, JSON: true})

	logger.Info("test", "key", "value")

	out := buf.String()
	if !strings.Contains(out, `"msg"`) {
		t.Fatalf("expected JSON format, got: %s", out)
	}
	if !strings.Contains(out, `"key"`) {
		t.Fatalf("expected key in JSON output, got: %s", out)
	}
}

func TestNew_StructuredAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Output: &buf})

	logger.Info("tool started", "tool", "whisper", "node", "node-a")

	out := buf.String()
	if !strings.Contains(out, "whisper") {
		t.Fatal("expected 'whisper' in output")
	}
	if !strings.Contains(out, "node-a") {
		t.Fatal("expected 'node-a' in output")
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},        // default
		{"unknown", slog.LevelInfo}, // default
		{" Info ", slog.LevelInfo},  // trimmed
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := parseLevel(tt.input); got != tt.want {
				t.Fatalf("parseLevel(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSetDefault(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Config{Output: &buf})
	SetDefault(logger)

	// After SetDefault, slog.Info should write to our buffer
	slog.Info("default test")

	if !strings.Contains(buf.String(), "default test") {
		t.Fatal("SetDefault should redirect slog.Info to our logger")
	}
}
