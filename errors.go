package gorker

import (
	"errors"
	"fmt"
	"runtime/debug"
)

var (
	ErrNilContext            = errors.New("worker: nil context")
	ErrNilWorker             = errors.New("worker: nil implementation")
	ErrNilMode               = errors.New("worker: nil mode")
	ErrNilLogger             = errors.New("worker: nil logger")
	ErrEmptyName             = errors.New("worker: name must not be empty")
	ErrInvalidWorkerName     = errors.New("worker: name must contain only lowercase ASCII letters, digits, and underscores")
	ErrInvalidTimeout        = errors.New("worker: timeout must not be negative")
	ErrInvalidInterval       = errors.New("worker: interval must be positive")
	ErrInvalidDelay          = errors.New("worker: delay must be positive")
	ErrInvalidScheduledTime  = errors.New("worker: scheduled time must not be zero")
	ErrInvalidCronExpression = errors.New("worker: invalid cron expression")
	ErrInvalidRetryAttempts  = errors.New("worker: retry requires at least two attempts when enabled")
	ErrInvalidRetryDelay     = errors.New("worker: retry delay is invalid")
)

type PanicError struct {
	Value any
	Stack []byte
}

func newPanicError(value any) *PanicError {
	return &PanicError{
		Value: value,
		Stack: debug.Stack(),
	}
}

func (e *PanicError) Error() string {
	return fmt.Sprint("worker panicked: ", e.Value)
}
