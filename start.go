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
// Validation errors are returned synchronously and no goroutine is started.
// On success the channel is closed without a value. If a running worker fails,
// exactly one error is sent before the channel is closed.
func Start(
	ctx context.Context,
	name string,
	implementation Worker,
	mode Mode,
	config Config,
) (Result, error) {
	if config.Retry.MaxAttempts == 0 {
		config.Retry.MaxAttempts = 1
	}
	if err := validateStart(ctx, name, implementation, mode, config); err != nil {
		return nil, err
	}

	result := make(chan error, 1)
	execution := executionConfig{
		timeout: config.Timeout,
		retry:   config.Retry,
		logger:  config.Logger,
	}

	go func() {
		defer close(result)
		startedAt := time.Now()
		execution.logger.Info(
			"worker started",
			"worker", name,
			"timeout", execution.timeout,
			"max_attempts", execution.retry.MaxAttempts,
		)

		executeRun := func(executionContext context.Context) error {
			return executeWithPolicy(
				executionContext,
				name,
				implementation,
				&execution,
			)
		}

		err := mode.run(ctx, name, execution.logger, executeRun)
		if err == nil {
			execution.logger.Info(
				"worker completed",
				"worker", name,
				"duration", time.Since(startedAt),
			)
			return
		}

		duration := time.Since(startedAt)
		var panicErr *PanicError
		if errors.Is(err, context.Canceled) || (ctx.Err() != nil && errors.Is(err, ctx.Err())) {
			execution.logger.Info(
				"worker stopped",
				"worker", name,
				"duration", duration,
				"reason", cancellationReason(err),
				"error", err,
			)
		} else if errors.As(err, &panicErr) {
			execution.logger.Error(
				"worker panicked",
				"worker", name,
				"duration", duration,
				"error", err,
				"panic_value", panicErr.Value,
				"stack", string(panicErr.Stack),
			)
		} else {
			execution.logger.Error(
				"worker failed",
				"worker", name,
				"duration", duration,
				"error", err,
			)
		}
		result <- err
	}()

	return result, nil
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
	if isNil(config.Logger) {
		return ErrNilLogger
	}
	if config.Timeout < 0 {
		return ErrInvalidTimeout
	}
	if err := config.Retry.validate(); err != nil {
		return err
	}
	return mode.validate()
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
