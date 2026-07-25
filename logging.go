package gorker

import (
	"context"
	"errors"
)

type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}

func cancellationReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "context_canceled"
}
