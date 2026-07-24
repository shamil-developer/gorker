package gorker

import (
	"context"
	"errors"
	"time"
)

// Logger receives structured worker lifecycle events.
type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}

const (
	eventWorkerStartRejected  = "worker.start_rejected"
	eventModeValidationPanic  = "worker.mode_validation_panicked"
	eventWorkerStarted        = "worker.started"
	eventWorkerCompleted      = "worker.completed"
	eventWorkerStopped        = "worker.stopped"
	eventWorkerFailed         = "worker.failed"
	eventWorkerModePanicked   = "worker.mode_panicked"
	eventAttemptStarted       = "worker.attempt.started"
	eventAttemptCompleted     = "worker.attempt.completed"
	eventAttemptCanceled      = "worker.attempt.canceled"
	eventAttemptFailed        = "worker.attempt.failed"
	eventAttemptPanicked      = "worker.attempt.panicked"
	eventWorkerRetryScheduled = "worker.retry.scheduled"
)

type lifecycleReporter struct {
	logger Logger
}

func (r lifecycleReporter) startRejected(name string, err error) {
	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		r.logger.Error(
			"worker mode validation panicked",
			"event", eventModeValidationPanic,
			"worker", name,
			"error", err,
			"stack", string(panicErr.Stack),
		)
		return
	}

	r.logger.Error(
		"worker start rejected",
		"event", eventWorkerStartRejected,
		"worker", name,
		"error", err,
	)
}

func (r lifecycleReporter) workerStarted(
	name string,
	mode string,
	timeout time.Duration,
	maxAttempts int,
) {
	r.logger.Info(
		"worker started",
		"event", eventWorkerStarted,
		"worker", name,
		"mode", mode,
		"timeout", timeout,
		"max_attempts", maxAttempts,
	)
}

func (r lifecycleReporter) workerCompleted(name string, duration time.Duration) {
	r.logger.Info(
		"worker completed",
		"event", eventWorkerCompleted,
		"worker", name,
		"duration", duration,
	)
}

func (r lifecycleReporter) workerStopped(
	name string,
	duration time.Duration,
	err error,
) {
	r.logger.Info(
		"worker stopped",
		"event", eventWorkerStopped,
		"worker", name,
		"duration", duration,
		"reason", cancellationReason(err),
		"error", err,
	)
}

func (r lifecycleReporter) workerFailed(
	name string,
	duration time.Duration,
	err error,
) {
	r.logger.Error(
		"worker failed",
		"event", eventWorkerFailed,
		"worker", name,
		"duration", duration,
		"error", err,
	)
}

func (r lifecycleReporter) modePanicked(
	name string,
	duration time.Duration,
	err *PanicError,
) {
	r.logger.Error(
		"worker mode panicked",
		"event", eventWorkerModePanicked,
		"worker", name,
		"duration", duration,
		"error", err,
		"stack", string(err.Stack),
	)
}

func (r lifecycleReporter) attemptStarted(name string, run uint64, attempt int) {
	r.logger.Debug(
		"worker attempt started",
		"event", eventAttemptStarted,
		"worker", name,
		"run", run,
		"attempt", attempt,
	)
}

func (r lifecycleReporter) attemptCompleted(
	name string,
	run uint64,
	attempt int,
	duration time.Duration,
) {
	r.logger.Info(
		"worker attempt completed",
		"event", eventAttemptCompleted,
		"worker", name,
		"run", run,
		"attempt", attempt,
		"duration", duration,
	)
}

func (r lifecycleReporter) retrying(
	name string,
	run uint64,
	attempt int,
	duration time.Duration,
	err error,
	delay time.Duration,
) {
	r.logger.Warn(
		"worker attempt failed; retry scheduled",
		"event", eventWorkerRetryScheduled,
		"worker", name,
		"run", run,
		"attempt", attempt,
		"duration", duration,
		"error", err,
		"retry_in", delay,
	)
}

func (r lifecycleReporter) attemptFailed(
	name string,
	run uint64,
	attempt int,
	duration time.Duration,
	err error,
) {
	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		r.logger.Error(
			"worker attempt panicked",
			"event", eventAttemptPanicked,
			"worker", name,
			"run", run,
			"attempt", attempt,
			"duration", duration,
			"error", err,
			"stack", string(panicErr.Stack),
		)
		return
	}

	r.logger.Error(
		"worker attempt failed",
		"event", eventAttemptFailed,
		"worker", name,
		"run", run,
		"attempt", attempt,
		"duration", duration,
		"error", err,
	)
}

func (r lifecycleReporter) attemptCanceled(
	name string,
	run uint64,
	attempt int,
	duration time.Duration,
	err error,
) {
	r.logger.Info(
		"worker attempt canceled",
		"event", eventAttemptCanceled,
		"worker", name,
		"run", run,
		"attempt", attempt,
		"duration", duration,
		"reason", cancellationReason(err),
		"error", err,
	)
}

func cancellationReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "context_canceled"
}
