package gorker

import (
	"context"
	"errors"
)

// Logger records structured worker lifecycle events.
//
// Args are alternating key-value pairs. Implementations must be safe for
// concurrent use. The standard library *slog.Logger satisfies Logger.
type Logger interface {
	// Debug records diagnostic scheduling and execution details.
	Debug(message string, args ...any)

	// Info records successful lifecycle transitions and normal shutdown.
	Info(message string, args ...any)

	// Warn records retry scheduling, skipped executions, and recoverable errors.
	Warn(message string, args ...any)

	// Error records terminal execution failures and recovered panics.
	Error(message string, args ...any)
}

func cancellationReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "context_canceled"
}
