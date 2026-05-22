// Package log provides default *slog.Logger constructors used by the engine
// when the caller does not supply one via WithLogger.
//
// The library uses the standard library log/slog.Logger as its logging
// contract. Any structured logger that exposes a slog.Handler (zap via
// go.uber.org/zap/exp/zapslog, zerolog via samber/slog-zerolog, logrus via
// samber/slog-logrus, etc.) can be plugged in without an adapter.
package log

import (
	"io"
	"log/slog"
	"os"
)

// Default returns a JSON slog logger writing to stderr at info level. This is
// the logger used by the engine when no WithLogger option is supplied.
func Default() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// Nop returns a logger that discards all output. Useful for tests.
func Nop() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
