package gorker

import (
	"context"
	"errors"
	"time"
)

func executeWithPolicy(
	ctx context.Context,
	name string,
	implementation Worker,
	config *executionConfig,
) error {
	config.runCounter++
	run := config.runCounter
	maxAttempts := config.retry.attempts()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptStartedAt := time.Now()
		config.reporter.attemptStarted(name, run, attempt)
		err := executeAttempt(ctx, implementation, config)
		attemptDuration := time.Since(attemptStartedAt)
		if err == nil {
			config.reporter.attemptCompleted(
				name,
				run,
				attempt,
				attemptDuration,
			)
			return nil
		}

		if parentErr := ctx.Err(); parentErr != nil {
			config.reporter.attemptCanceled(
				name,
				run,
				attempt,
				attemptDuration,
				parentErr,
			)
			return parentErr
		}
		if errors.Is(err, context.Canceled) {
			config.reporter.attemptCanceled(
				name,
				run,
				attempt,
				attemptDuration,
				err,
			)
			return err
		}
		var panicErr *PanicError
		if errors.As(err, &panicErr) {
			config.reporter.attemptFailed(
				name,
				run,
				attempt,
				attemptDuration,
				err,
			)
			return err
		}
		if attempt == maxAttempts {
			config.reporter.attemptFailed(
				name,
				run,
				attempt,
				attemptDuration,
				err,
			)
			return err
		}

		delay := config.retry.nextDelay(attempt)
		config.reporter.retrying(
			name,
			run,
			attempt,
			attemptDuration,
			err,
			delay,
		)
		if err := waitDuration(ctx, delay); err != nil {
			return err
		}
	}

	return nil
}

type executionConfig struct {
	timeout    time.Duration
	retry      Retry
	reporter   lifecycleReporter
	runCounter uint64
}

func prepareExecutionConfig(config Config) (executionConfig, error) {
	prepared := executionConfig{
		timeout: config.Timeout,
		retry:   config.Retry,
		reporter: lifecycleReporter{
			logger: config.Logger,
		},
	}
	if isNil(config.Logger) {
		return prepared, ErrNilLogger
	}
	if config.Timeout < 0 {
		return prepared, ErrInvalidTimeout
	}
	if err := config.Retry.validate(); err != nil {
		return prepared, err
	}
	return prepared, nil
}

func executeAttempt(
	parent context.Context,
	implementation Worker,
	config *executionConfig,
) (err error) {
	ctx := parent
	cancel := func() {}
	if config.timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, config.timeout)
	}
	defer cancel()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = newPanicError(recovered)
		}
	}()

	err = implementation.Execute(ctx)
	if err == nil && ctx.Err() != nil {
		return ctx.Err()
	}

	return err
}
