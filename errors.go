package gorker

import (
	"errors"
	"fmt"
	"runtime/debug"
)

var (
	errNilContext            = errors.New("worker: nil context")
	errNilWorker             = errors.New("worker: nil implementation")
	errNilMode               = errors.New("worker: nil mode")
	errNilLogger             = errors.New("worker: nil logger")
	errEmptyName             = errors.New("worker: name must not be empty")
	errInvalidWorkerName     = errors.New("worker: name must contain only lowercase ASCII letters, digits, and underscores")
	errInvalidTimeout        = errors.New("worker: timeout must not be negative")
	errInvalidInterval       = errors.New("worker: interval must be positive")
	errInvalidDelay          = errors.New("worker: delay must be positive")
	errInvalidScheduledTime  = errors.New("worker: scheduled time must not be zero")
	errInvalidCronExpression = errors.New("worker: invalid cron expression")
	errInvalidRetryAttempts  = errors.New("worker: retry requires at least two attempts when enabled")
	errInvalidRetryDelay     = errors.New("worker: retry delay is invalid")
)

// PanicError reports a panic recovered from a Worker execution.
//
// A caller can identify a recovered panic with errors.As. The panic value and
// stack trace are written to the configured Logger.
type PanicError struct {
	value any
	stack []byte
}

func newPanicError(value any) *PanicError {
	return &PanicError{
		value: value,
		stack: debug.Stack(),
	}
}

// Error returns a description of the recovered panic.
func (e *PanicError) Error() string {
	return fmt.Sprint("worker panicked: ", e.value)
}
