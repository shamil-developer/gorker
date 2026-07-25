package gorker

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// Result reports the final outcome of an asynchronously running worker.
//
// On success, Result is closed without a value. On failure, it receives one
// error and is then closed. A Result must have only one consumer.
type Result <-chan error

// Mode controls when a worker is executed.
//
// Only the built-in modes provided by this package implement Mode.
type Mode interface {
	validate() error
	run(
		ctx context.Context,
		name string,
		logger Logger,
		execute func(context.Context) error,
	) error
}

// Start validates and starts a worker in a new goroutine.
//
// Validation errors are returned synchronously, and no goroutine is started.
// A valid name contains only lowercase ASCII letters, digits, and underscores.
//
// On success, Start returns a Result immediately. Runtime failures are reported
// through that Result. Canceling ctx stops future executions and asks the
// current execution to stop cooperatively.
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
				"panic_value", panicErr.value,
				"stack", string(panicErr.stack),
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

// Wait waits for every Result or until ctx is canceled.
//
// Worker errors are combined with errors.Join. A worker reporting
// context.Canceled is treated as a normal shutdown. Nil results are ignored.
// Each Result passed to Wait must not be read elsewhere.
func Wait(ctx context.Context, results ...Result) error {
	if ctx == nil {
		return errNilContext
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
		return errNilContext
	}
	if name == "" {
		return errEmptyName
	}
	if !isValidWorkerName(name) {
		return errInvalidWorkerName
	}
	if isNilInterface(implementation) {
		return errNilWorker
	}
	if isNilInterface(mode) {
		return errNilMode
	}
	if isNilInterface(config.Logger) {
		return errNilLogger
	}
	if config.Timeout < 0 {
		return errInvalidTimeout
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
