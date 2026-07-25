package gorker

import (
	"context"
	"errors"
	"time"
)

// Result reports the final asynchronous worker error.
// On success it is closed without a value.
type Result <-chan error

// Start starts a worker in a new goroutine and immediately returns its result.
//
// On success the channel is closed without a value. On failure exactly one error
// is sent before the channel is closed.
func Start(
	ctx context.Context,
	name string,
	implementation Worker,
	mode Mode,
	config Config,
) Result {
	result := make(chan error, 1)
	if isNil(config.Logger) {
		result <- ErrNilLogger
		close(result)
		return result
	}

	if err := validateStart(ctx, name, implementation, mode, config); err != nil {
		config.Logger.Error(
			"worker start rejected",
			"worker", name,
			"error", err,
		)
		result <- err
		close(result)
		return result
	}

	preparedConfig := executionConfig{
		timeout: config.Timeout,
		retry:   config.Retry,
		logger:  config.Logger,
	}

	go func() {
		defer close(result)
		startedAt := time.Now()
		preparedConfig.logger.Info(
			"worker started",
			"worker", name,
			"timeout", preparedConfig.timeout,
			"max_attempts", preparedConfig.retry.attempts(),
		)

		execute := func(executionContext context.Context) error {
			return executeWithPolicy(
				executionContext,
				name,
				implementation,
				&preparedConfig,
			)
		}

		err := mode.run(ctx, name, preparedConfig.logger, execute)
		if err == nil {
			preparedConfig.logger.Info(
				"worker completed",
				"worker", name,
				"duration", time.Since(startedAt),
			)
			return
		}

		duration := time.Since(startedAt)
		switch {
		case isContextTermination(ctx, err):
			preparedConfig.logger.Info(
				"worker stopped",
				"worker", name,
				"duration", duration,
				"reason", contextTerminationReason(err),
				"error", err,
			)
		default:
			var panicErr *PanicError
			if errors.As(err, &panicErr) {
				preparedConfig.logger.Error(
					"worker panicked",
					"worker", name,
					"duration", duration,
					"error", err,
					"panic_value", panicErr.Value,
					"stack", string(panicErr.Stack),
				)
			} else {
				preparedConfig.logger.Error(
					"worker failed",
					"worker", name,
					"duration", duration,
					"error", err,
				)
			}
		}
		result <- err
	}()

	return result
}

func validateStart(
	ctx context.Context,
	name string,
	implementation Worker,
	mode Mode,
	config Config,
) error {
	if ctx == nil {
		return ErrNilContext
	}
	if name == "" {
		return ErrEmptyName
	}
	if !isValidWorkerName(name) {
		return ErrInvalidWorkerName
	}
	if isNil(implementation) {
		return ErrNilWorker
	}
	if isNil(mode) {
		return ErrNilMode
	}
	if config.Timeout < 0 {
		return ErrInvalidTimeout
	}
	if err := config.Retry.validate(); err != nil {
		return err
	}
	return mode.validate()
}

func isContextTermination(ctx context.Context, err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	return ctx.Err() != nil && errors.Is(err, ctx.Err())
}

func isValidWorkerName(name string) bool {
	for index := range len(name) {
		character := name[index]
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '_' {
			continue
		}
		return false
	}
	return true
}

func contextTerminationReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "context_canceled"
}
