package gorker

import (
	"context"
	"errors"
	"time"
)

// Worker contains only the business operation. Start controls its lifecycle.
type Worker interface {
	Execute(ctx context.Context) error
}

// Config controls how each worker execution is handled.
type Config struct {
	// Timeout limits each individual attempt. Zero disables it.
	Timeout time.Duration

	// Retry configures repeated attempts. Its zero value means one attempt.
	Retry Retry

	// Logger receives structured worker logs.
	// It is required.
	Logger Logger
}

func executeWithPolicy(
	ctx context.Context,
	name string,
	implementation Worker,
	config *executionConfig,
) error {
	config.runCounter++
	run := config.runCounter
	maxAttempts := config.retry.MaxAttempts

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptStartedAt := time.Now()
		config.logger.Debug(
			"worker attempt started",
			"worker", name,
			"run", run,
			"attempt", attempt,
		)
		err := executeAttempt(ctx, implementation, config.timeout)
		attemptDuration := time.Since(attemptStartedAt)
		if err == nil {
			config.logger.Info(
				"worker attempt completed",
				"worker", name,
				"run", run,
				"attempt", attempt,
				"duration", attemptDuration,
			)
			return nil
		}

		if parentErr := ctx.Err(); parentErr != nil {
			config.logger.Info(
				"worker attempt canceled",
				"worker", name,
				"run", run,
				"attempt", attempt,
				"duration", attemptDuration,
				"reason", cancellationReason(parentErr),
				"error", parentErr,
			)
			return parentErr
		}
		if errors.Is(err, context.Canceled) {
			config.logger.Info(
				"worker attempt canceled",
				"worker", name,
				"run", run,
				"attempt", attempt,
				"duration", attemptDuration,
				"reason", cancellationReason(err),
				"error", err,
			)
			return err
		}
		var panicErr *PanicError
		if errors.As(err, &panicErr) {
			config.logger.Error(
				"worker attempt panicked",
				"worker", name,
				"run", run,
				"attempt", attempt,
				"duration", attemptDuration,
				"error", err,
				"panic_value", panicErr.Value,
				"stack", string(panicErr.Stack),
			)
			return err
		}
		if attempt == maxAttempts {
			if errors.Is(err, context.DeadlineExceeded) {
				config.logger.Error(
					"worker attempt timed out",
					"worker", name,
					"run", run,
					"attempt", attempt,
					"duration", attemptDuration,
					"timeout", config.timeout,
					"error", err,
				)
				return err
			}
			config.logger.Error(
				"worker attempt failed",
				"worker", name,
				"run", run,
				"attempt", attempt,
				"duration", attemptDuration,
				"error", err,
			)
			return err
		}

		delay := config.retry.nextDelay(attempt)
		if errors.Is(err, context.DeadlineExceeded) {
			config.logger.Warn(
				"worker attempt timed out; retry scheduled",
				"worker", name,
				"run", run,
				"attempt", attempt,
				"duration", attemptDuration,
				"timeout", config.timeout,
				"error", err,
				"retry_in", delay,
			)
		} else {
			config.logger.Warn(
				"worker attempt failed; retry scheduled",
				"worker", name,
				"run", run,
				"attempt", attempt,
				"duration", attemptDuration,
				"error", err,
				"retry_in", delay,
			)
		}
		if err := waitDuration(ctx, delay); err != nil {
			config.logger.Info(
				"worker retry canceled",
				"worker", name,
				"run", run,
				"after_attempt", attempt,
				"reason", cancellationReason(err),
				"error", err,
			)
			return err
		}
	}

	return nil
}

type executionConfig struct {
	timeout    time.Duration
	retry      Retry
	logger     Logger
	runCounter uint64
}

func executeAttempt(
	parent context.Context,
	implementation Worker,
	timeout time.Duration,
) (err error) {
	ctx := parent
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	}
	defer cancel()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = newPanicError(recovered)
		}
	}()

	err = implementation.Execute(ctx)
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}

	return err
}
