// Package logging provides structured logging for jb-mesh using log/slog.
//
// Usage:
//
//	logger := logging.New(logging.Config{Level: "debug"})
//	logger.Info("tool started", "tool", name, "node", nodeName)
//	logger.Error("tool crashed", "tool", name, "err", err)
//
// The package provides a default logger via package-level functions:
//
//	logging.SetDefault(logger)
//	logging.Info("connected", "url", natsURL)
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Config holds logging configuration.
type Config struct {
	// Level is the minimum log level: "debug", "info", "warn", "error".
	// Default: "info".
	Level string

	// Output is the writer for log output. Default: os.Stderr.
	Output io.Writer

	// JSON enables JSON-formatted output instead of text.
	JSON bool
}

// New creates a new slog.Logger with the given configuration.
func New(cfg Config) *slog.Logger {
	level := parseLevel(cfg.Level)

	output := cfg.Output
	if output == nil {
		output = os.Stderr
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var handler slog.Handler
	if cfg.JSON {
		handler = slog.NewJSONHandler(output, opts)
	} else {
		handler = slog.NewTextHandler(output, opts)
	}

	return slog.New(handler)
}

// SetDefault sets the default slog logger used by the package-level functions
// and by the standard log package.
func SetDefault(logger *slog.Logger) {
	slog.SetDefault(logger)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
