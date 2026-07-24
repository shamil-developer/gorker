package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestExecutionSucceedsOnFirstAttempt(t *testing.T) {
	calls := 0
	err := executeWithPolicy(
		context.Background(),
		"success",
		workerFunc(func(context.Context) error {
			calls++
			return nil
		}),
		testExecutionConfig(t, Config{Logger: &recordingLogger{}}),
	)

	if err != nil {
		t.Fatalf("executeWithPolicy() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestExecutionAlreadyCanceledContextSkipsWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0

	err := executeWithPolicy(
		ctx,
		"canceled",
		workerFunc(func(context.Context) error {
			calls++
			return nil
		}),
		testExecutionConfig(t, Config{Logger: &recordingLogger{}}),
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("executeWithPolicy() error = %v, want Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("calls = %d, want 0", calls)
	}
}

func TestExecutionRetriesUntilSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var attempts []time.Time

		err := executeWithPolicy(
			t.Context(),
			"retry",
			workerFunc(func(context.Context) error {
				attempts = append(attempts, time.Now())
				if len(attempts) < 3 {
					return errors.New("temporary")
				}
				return nil
			}),
			testExecutionConfig(t, Config{
				Retry: Retry{
					MaxAttempts:  3,
					InitialDelay: time.Second,
					MaxDelay:     10 * time.Second,
				},
				Logger: &recordingLogger{},
			}),
		)

		if err != nil {
			t.Fatalf("executeWithPolicy() error = %v", err)
		}
		if len(attempts) != 3 {
			t.Fatalf("attempts = %d, want 3", len(attempts))
		}
		if got := attempts[2].Sub(start); got != 3*time.Second {
			t.Fatalf("third attempt started after %v, want 3s", got)
		}
	})
}

func TestExecutionReturnsLastErrorAfterAttemptsExhausted(t *testing.T) {
	firstErr := errors.New("first")
	lastErr := errors.New("last")
	calls := 0

	err := executeWithPolicy(
		context.Background(),
		"failure",
		workerFunc(func(context.Context) error {
			calls++
			if calls == 1 {
				return firstErr
			}
			return lastErr
		}),
		testExecutionConfig(t, Config{
			Retry: Retry{
				MaxAttempts:  2,
				InitialDelay: time.Nanosecond,
				MaxDelay:     time.Nanosecond,
			},
			Logger: &recordingLogger{},
		}),
	)

	if !errors.Is(err, lastErr) {
		t.Fatalf("executeWithPolicy() error = %v, want last error", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestExecutionTimeoutAppliesToEveryAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		err := executeWithPolicy(
			t.Context(),
			"timeout",
			workerFunc(func(ctx context.Context) error {
				attempts++
				<-ctx.Done()
				return ctx.Err()
			}),
			testExecutionConfig(t, Config{
				Timeout: time.Second,
				Retry: Retry{
					MaxAttempts:  2,
					InitialDelay: time.Second,
					MaxDelay:     time.Second,
				},
				Logger: &recordingLogger{},
			}),
		)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("executeWithPolicy() error = %v, want DeadlineExceeded", err)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want 2", attempts)
		}
	})
}

func TestExecutionReturnsDeadlineWhenWorkerFinishesAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		err := executeWithPolicy(
			t.Context(),
			"late-success",
			workerFunc(func(context.Context) error {
				time.Sleep(2 * time.Second)
				return nil
			}),
			testExecutionConfig(t, Config{
				Timeout: time.Second,
				Logger:  &recordingLogger{},
			}),
		)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("executeWithPolicy() error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestExecutionCancellationInterruptsRetryDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		firstAttempt := make(chan struct{})
		done := make(chan error, 1)
		config := testExecutionConfig(t, Config{
			Retry: Retry{
				MaxAttempts:  2,
				InitialDelay: time.Hour,
				MaxDelay:     time.Hour,
			},
			Logger: &recordingLogger{},
		})

		go func() {
			done <- executeWithPolicy(
				ctx,
				"cancel",
				workerFunc(func(context.Context) error {
					close(firstAttempt)
					return errors.New("retry")
				}),
				config,
			)
		}()

		<-firstAttempt
		cancel()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("executeWithPolicy() error = %v, want Canceled", err)
		}
	})
}

func TestExecutionDoesNotRetryCanceledError(t *testing.T) {
	calls := 0
	err := executeWithPolicy(
		context.Background(),
		"canceled",
		workerFunc(func(context.Context) error {
			calls++
			return context.Canceled
		}),
		testExecutionConfig(t, Config{
			Retry: Retry{
				MaxAttempts:  3,
				InitialDelay: time.Second,
				MaxDelay:     time.Second,
			},
			Logger: &recordingLogger{},
		}),
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("executeWithPolicy() error = %v, want Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestExecutionDoesNotRetryPanic(t *testing.T) {
	calls := 0
	err := executeWithPolicy(
		context.Background(),
		"panic",
		workerFunc(func(context.Context) error {
			calls++
			panic("boom")
		}),
		testExecutionConfig(t, Config{
			Retry: Retry{
				MaxAttempts:  3,
				InitialDelay: time.Second,
				MaxDelay:     time.Second,
			},
			Logger: &recordingLogger{},
		}),
	)

	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("executeWithPolicy() error = %T, want *PanicError", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if len(panicErr.Stack) == 0 {
		t.Fatal("PanicError.Stack is empty")
	}
}

func TestExecutionLogsLifecycle(t *testing.T) {
	logger := &recordingLogger{}
	calls := 0
	err := executeWithPolicy(
		context.Background(),
		"logging",
		workerFunc(func(context.Context) error {
			calls++
			if calls == 1 {
				return errors.New("temporary")
			}
			return nil
		}),
		testExecutionConfig(t, Config{
			Retry: Retry{
				MaxAttempts:  2,
				InitialDelay: time.Nanosecond,
				MaxDelay:     time.Nanosecond,
			},
			Logger: logger,
		}),
	)

	if err != nil {
		t.Fatalf("executeWithPolicy() error = %v", err)
	}

	entries := logger.snapshot()
	wantEntries := []struct {
		level   string
		message string
		event   string
		attempt int
	}{
		{"debug", "worker attempt started", eventAttemptStarted, 1},
		{"warn", "worker attempt failed; retry scheduled", eventWorkerRetryScheduled, 1},
		{"debug", "worker attempt started", eventAttemptStarted, 2},
		{"info", "worker attempt completed", eventAttemptCompleted, 2},
	}
	if len(entries) != len(wantEntries) {
		t.Fatalf("log entries = %d, want %d", len(entries), len(wantEntries))
	}
	for index, want := range wantEntries {
		entry := entries[index]
		if entry.level != want.level {
			t.Errorf("entry %d level = %q, want %q", index, entry.level, want.level)
		}
		if entry.message != want.message {
			t.Errorf("entry %d message = %q, want %q", index, entry.message, want.message)
		}
		if event, _ := entry.value("event"); event != want.event {
			t.Errorf("entry %d event = %v, want %q", index, event, want.event)
		}
		if run, _ := entry.value("run"); run != uint64(1) {
			t.Errorf("entry %d run = %v, want 1", index, run)
		}
		if attempt, _ := entry.value("attempt"); attempt != want.attempt {
			t.Errorf("entry %d attempt = %v, want %d", index, attempt, want.attempt)
		}
	}

	retryDelay, exists := entries[1].value("retry_in")
	if !exists || retryDelay != time.Nanosecond {
		t.Fatalf("retry_in = %v, want 1ns", retryDelay)
	}
	if _, exists := entries[1].value("duration"); !exists {
		t.Fatal("retry entry has no duration")
	}
	if _, exists := entries[3].value("duration"); !exists {
		t.Fatal("completion entry has no duration")
	}
}

func TestExecutionIncrementsRunNumber(t *testing.T) {
	logger := &recordingLogger{}
	config := testExecutionConfig(t, Config{Logger: logger})
	implementation := workerFunc(func(context.Context) error { return nil })

	for range 2 {
		if err := executeWithPolicy(
			context.Background(),
			"runs",
			implementation,
			config,
		); err != nil {
			t.Fatalf("executeWithPolicy() error = %v", err)
		}
	}

	entries := logger.snapshot()
	if len(entries) != 4 {
		t.Fatalf("log entries = %d, want 4", len(entries))
	}
	firstRun, _ := entries[0].value("run")
	secondRun, _ := entries[2].value("run")
	if firstRun != uint64(1) || secondRun != uint64(2) {
		t.Fatalf("run numbers = %v, %v; want 1, 2", firstRun, secondRun)
	}
}

func testExecutionConfig(t *testing.T, config Config) *executionConfig {
	t.Helper()

	prepared, err := prepareExecutionConfig(config)
	if err != nil {
		t.Fatalf("prepareExecutionConfig() error = %v", err)
	}
	return &prepared
}
