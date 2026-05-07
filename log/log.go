package log

import (
	"log/slog"
	"os"
)

// Logger is the interface for structured logging.
// Any implementation (e.g. zap, zerolog) can satisfy this interface.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

// Nop returns a no-op logger that discards all output.
func Nop() Logger { return &nopLogger{} }

type nopLogger struct{}

func (n *nopLogger) Info(string, ...any)  {}
func (n *nopLogger) Warn(string, ...any)  {}
func (n *nopLogger) Error(string, ...any) {}
func (n *nopLogger) Debug(string, ...any) {}

// New creates a default structured logger writing JSON to stderr.
func New() Logger {
	return &defaultLogger{
		l: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
}

// defaultLogger wraps slog.
type defaultLogger struct {
	l *slog.Logger
}

func (d *defaultLogger) Info(msg string, args ...any)  { d.l.Info(msg, args...) }
func (d *defaultLogger) Warn(msg string, args ...any)  { d.l.Warn(msg, args...) }
func (d *defaultLogger) Error(msg string, args ...any) { d.l.Error(msg, args...) }
func (d *defaultLogger) Debug(msg string, args ...any) { d.l.Debug(msg, args...) }
