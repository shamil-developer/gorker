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
	preparedConfig, err := prepareExecutionConfig(config)
	if err != nil {
		if !errors.Is(err, ErrNilLogger) {
			lifecycleReporter{logger: config.Logger}.startRejected(name, err)
		}
		result <- err
		close(result)
		return result
	}
	reporter := preparedConfig.reporter

	if err := validateStart(ctx, name, implementation, mode); err != nil {
		reporter.startRejected(name, err)
		result <- err
		close(result)
		return result
	}

	go func() {
		defer close(result)
		startedAt := time.Now()
		reporter.workerStarted(
			name,
			modeName(mode),
			preparedConfig.timeout,
			preparedConfig.retry.attempts(),
		)

		execute := func(executionContext context.Context) error {
			err := executeWithPolicy(
				executionContext,
				name,
				implementation,
				&preparedConfig,
			)
			if err != nil {
				return &executionFailure{err: err}
			}
			return nil
		}

		err := runMode(ctx, mode, execute)
		if err == nil {
			reporter.workerCompleted(name, time.Since(startedAt))
			return
		}

		finalErr := err
		var failure *executionFailure
		executionFailed := errors.As(err, &failure)
		if executionFailed {
			finalErr = failure.err
		}

		duration := time.Since(startedAt)
		switch {
		case isContextTermination(ctx, finalErr):
			reporter.workerStopped(name, duration, finalErr)
		case executionFailed:
			reporter.workerFailed(name, duration, finalErr)
		default:
			var panicErr *PanicError
			if errors.As(finalErr, &panicErr) {
				reporter.modePanicked(name, duration, panicErr)
			} else {
				reporter.workerFailed(name, duration, finalErr)
			}
		}
		result <- finalErr
	}()

	return result
}

func validateStart(
	ctx context.Context,
	name string,
	implementation Worker,
	mode Mode,
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
	return validateMode(mode)
}

func validateMode(mode Mode) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newPanicError(recovered)
		}
	}()

	return mode.validate()
}

func modeName(mode Mode) string {
	modeType := reflect.TypeOf(mode)
	for modeType.Kind() == reflect.Pointer {
		modeType = modeType.Elem()
	}
	return modeType.String()
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

func runMode(
	ctx context.Context,
	mode Mode,
	execute func(context.Context) error,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newPanicError(recovered)
		}
	}()

	return mode.run(ctx, execute)
}

type executionFailure struct {
	err error
}

func (e *executionFailure) Error() string {
	return e.err.Error()
}
