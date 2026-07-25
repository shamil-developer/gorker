package gorker

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// Result reports the final asynchronous worker error.
// On success it is closed without a value.
type Result <-chan error

// Mode controls when a worker is executed.
//
// Only the built-in modes in this package implement Mode.
type Mode interface {
	validate() error
	run(
		ctx context.Context,
		name string,
		logger Logger,
		execute func(context.Context) error,
	) error
}

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

// Wait waits for all results or until ctx expires.
// Context cancellation reported by workers is treated as a normal shutdown.
func Wait(ctx context.Context, results ...Result) error {
	if ctx == nil {
		return ErrNilContext
	}

	var workerErrors []error
	for _, result := range results {
		if result == nil {
			continue
		}

		select {
		case err := <-result:
			if err != nil && !errors.Is(err, context.Canceled) {
				workerErrors = append(workerErrors, err)
			}

		case <-ctx.Done():
			workerErrors = append(workerErrors, ctx.Err())
			return errors.Join(workerErrors...)
		}
	}

	return errors.Join(workerErrors...)
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
	if isNilInterface(implementation) {
		return ErrNilWorker
	}
	if isNilInterface(mode) {
		return ErrNilMode
	}
	if isNilInterface(config.Logger) {
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

// isNilInterface detects both a nil interface and an interface holding a typed nil.
func isNilInterface(value any) bool {
	if value == nil {
		return true
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
