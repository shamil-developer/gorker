package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestStartReturnsBeforeWorkerCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		result, err := Start(
			t.Context(),
			"async",
			workerFunc(func(context.Context) error {
				close(started)
				<-release
				return nil
			}),
			Once{},
			Config{Logger: &recordingLogger{}},
		)
		if err != nil {
			t.Fatalf("Start() validation error = %v", err)
		}

		<-started
		select {
		case <-result:
			t.Fatal("Start completed before worker was released")
		default:
		}

		close(release)
		if err := <-result; err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	})
}

func TestStartReturnsWorkerErrorOnce(t *testing.T) {
	wantErr := errors.New("worker failed")
	logger := &recordingLogger{}
	result, err := Start(
		context.Background(),
		"failure",
		workerFunc(func(context.Context) error { return wantErr }),
		Once{},
		Config{Logger: logger},
	)
	if err != nil {
		t.Fatalf("Start() validation error = %v", err)
	}

	if err, ok := <-result; !ok || !errors.Is(err, wantErr) {
		t.Fatalf("first receive = (%v, %v), want worker error", err, ok)
	}
	if err, ok := <-result; ok || err != nil {
		t.Fatalf("second receive = (%v, %v), want closed channel", err, ok)
	}

	entries := logger.snapshot()
	lastEntry := entries[len(entries)-1]
	if lastEntry.message != "worker failed" {
		t.Fatalf("last message = %q, want worker failed", lastEntry.message)
	}
}

func TestStartRequiresLogger(t *testing.T) {
	var typedNil *recordingLogger
	tests := []struct {
		name   string
		logger Logger
	}{
		{name: "nil"},
		{name: "typed nil", logger: typedNil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			result, err := Start(
				context.Background(),
				"required_logger",
				workerFunc(func(context.Context) error {
					calls++
					return nil
				}),
				Once{},
				Config{Logger: test.logger},
			)

			if !errors.Is(err, ErrNilLogger) {
				t.Fatalf("Start() error = %v, want ErrNilLogger", err)
			}
			if result != nil {
				t.Fatal("Start() result must be nil after validation failure")
			}
			if calls != 0 {
				t.Fatalf("worker calls = %d, want 0", calls)
			}
		})
	}
}

func TestStartReturnsModeValidationError(t *testing.T) {
	logger := &recordingLogger{}
	wantErr := errors.New("invalid mode")
	runCalls := 0
	result, err := Start(
		context.Background(),
		"invalid_mode",
		workerFunc(func(context.Context) error {
			t.Fatal("worker executed after mode validation failure")
			return nil
		}),
		validationMode{
			validateFn: func() error { return wantErr },
			runFn: func(context.Context, func(context.Context) error) error {
				runCalls++
				return nil
			},
		},
		Config{Logger: logger},
	)

	if !errors.Is(err, wantErr) {
		t.Fatalf("Start() error = %v, want validation error", err)
	}
	if result != nil {
		t.Fatal("Start() result must be nil after validation failure")
	}
	if runCalls != 0 {
		t.Fatalf("Mode.run() calls = %d, want 0", runCalls)
	}
	if entries := logger.snapshot(); len(entries) != 0 {
		t.Fatalf("validation logs = %#v, want none", entries)
	}
}

func TestStartLogsStructuredWorkerLifecycle(t *testing.T) {
	logger := &recordingLogger{}
	result, err := Start(
		context.Background(),
		"lifecycle",
		workerFunc(func(context.Context) error { return nil }),
		&Once{},
		Config{
			Timeout: time.Second,
			Logger:  logger,
		},
	)
	if err != nil {
		t.Fatalf("Start() validation error = %v", err)
	}

	if err := <-result; err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	entries := logger.snapshot()
	if len(entries) != 5 {
		t.Fatalf("log entries = %#v, want five lifecycle entries", entries)
	}

	want := []struct {
		level   string
		message string
	}{
		{"info", "worker started"},
		{"debug", "worker once execution triggered"},
		{"debug", "worker attempt started"},
		{"info", "worker attempt completed"},
		{"info", "worker completed"},
	}
	for index, expected := range want {
		if entries[index].level != expected.level {
			t.Errorf(
				"entry %d level = %q, want %q",
				index,
				entries[index].level,
				expected.level,
			)
		}
		if entries[index].message != expected.message {
			t.Errorf(
				"entry %d message = %q, want %q",
				index,
				entries[index].message,
				expected.message,
			)
		}
		if worker, _ := entries[index].value("worker"); worker != "lifecycle" {
			t.Errorf("entry %d worker = %v, want lifecycle", index, worker)
		}
	}

	if timeout, _ := entries[0].value("timeout"); timeout != time.Second {
		t.Errorf("worker timeout = %v, want 1s", timeout)
	}
	if maxAttempts, _ := entries[0].value("max_attempts"); maxAttempts != 1 {
		t.Errorf("worker max_attempts = %v, want 1", maxAttempts)
	}
	if _, exists := entries[4].value("duration"); !exists {
		t.Fatal("worker completion entry has no duration")
	}
}

func TestStartLogsModeFailure(t *testing.T) {
	logger := &recordingLogger{}
	wantErr := errors.New("mode failed")
	result, err := Start(
		context.Background(),
		"mode_failure",
		workerFunc(func(context.Context) error { return nil }),
		modeFunc(func(context.Context, func(context.Context) error) error {
			return wantErr
		}),
		Config{Logger: logger},
	)
	if err != nil {
		t.Fatalf("Start() validation error = %v", err)
	}

	if err := <-result; !errors.Is(err, wantErr) {
		t.Fatalf("Start() error = %v, want mode error", err)
	}

	entries := logger.snapshot()
	if len(entries) != 2 {
		t.Fatalf("log entries = %#v, want worker start and failure entries", entries)
	}
	if entries[1].level != "error" {
		t.Fatalf("failure level = %q, want error", entries[1].level)
	}
	if entries[1].message != "worker failed" {
		t.Fatalf("failure message = %q, want worker failed", entries[1].message)
	}
	loggedValue, exists := entries[1].value("error")
	loggedErr, isError := loggedValue.(error)
	if !exists || !isError || !errors.Is(loggedErr, wantErr) {
		t.Fatalf("logged error = %v, want mode error", loggedValue)
	}
	if _, exists := entries[1].value("duration"); !exists {
		t.Fatal("worker failure entry has no duration")
	}
}

func TestStartLogsParentDeadlineAsStopReason(t *testing.T) {
	logger := &recordingLogger{}
	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	result, err := Start(
		ctx,
		"deadline",
		workerFunc(func(context.Context) error {
			t.Fatal("worker executed after parent deadline")
			return nil
		}),
		Once{},
		Config{Logger: logger},
	)
	if err != nil {
		t.Fatalf("Start() validation error = %v", err)
	}

	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want DeadlineExceeded", err)
	}

	entries := logger.snapshot()
	lastEntry := entries[len(entries)-1]
	if lastEntry.message != "worker stopped" {
		t.Fatalf("last message = %q, want worker stopped", lastEntry.message)
	}
	if reason, _ := lastEntry.value("reason"); reason != "deadline_exceeded" {
		t.Fatalf("stop reason = %v, want deadline_exceeded", reason)
	}
}

func TestStartOnceWorkerPanicReturnsError(t *testing.T) {
	calls := 0
	result, err := Start(
		context.Background(),
		"once_panic",
		workerFunc(func(context.Context) error {
			calls++
			panic("boom")
		}),
		Once{},
		Config{Logger: &recordingLogger{}},
	)
	if err != nil {
		t.Fatalf("Start() validation error = %v", err)
	}

	var panicErr *PanicError
	if err := <-result; !errors.As(err, &panicErr) {
		t.Fatalf("Start() error = %T %v, want *PanicError", err, err)
	}
	if calls != 1 {
		t.Fatalf("worker calls = %d, want 1", calls)
	}
}

func TestStartRecurringWorkerContinuesAfterPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		logger := &recordingLogger{}
		calls := 0

		result, err := Start(
			ctx,
			"panic_recovery",
			workerFunc(func(context.Context) error {
				calls++
				if calls == 1 {
					panic("temporary panic")
				}
				cancel()
				return nil
			}),
			Periodic{
				Interval:  time.Second,
				Immediate: true,
			},
			Config{Logger: logger},
		)
		if err != nil {
			t.Fatalf("Start() validation error = %v", err)
		}

		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Start() error = %v, want Canceled", err)
		}
		if calls != 2 {
			t.Fatalf("worker calls = %d, want 2", calls)
		}

		var panicLogged, continuationLogged bool
		for _, entry := range logger.snapshot() {
			switch entry.message {
			case "worker attempt panicked":
				panicLogged = true
			case "worker periodic execution failed; continuing":
				continuationLogged = true
			}
		}
		if !panicLogged || !continuationLogged {
			t.Fatalf(
				"panic log = %t, continuation log = %t; want both",
				panicLogged,
				continuationLogged,
			)
		}
	})
}

func TestStartTreatsModeCancellationAsNormalLifecycle(t *testing.T) {
	logger := &recordingLogger{}
	result, err := Start(
		context.Background(),
		"canceled_mode",
		workerFunc(func(context.Context) error { return nil }),
		modeFunc(func(context.Context, func(context.Context) error) error {
			return context.Canceled
		}),
		Config{Logger: logger},
	)
	if err != nil {
		t.Fatalf("Start() validation error = %v", err)
	}

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want Canceled", err)
	}
	entries := logger.snapshot()
	if len(entries) != 2 {
		t.Fatalf("log entries = %#v, want worker start and stop entries", entries)
	}
	if entries[0].level != "info" || entries[0].message != "worker started" {
		t.Fatalf("first log entry = %#v, want worker started info entry", entries[0])
	}
	if entries[1].level != "info" || entries[1].message != "worker stopped" {
		t.Fatalf("second log entry = %#v, want worker stopped info entry", entries[1])
	}
}

func TestStartValidation(t *testing.T) {
	validWorker := workerFunc(func(context.Context) error { return nil })
	validMode := Once{}
	validConfig := Config{Logger: &recordingLogger{}}

	var nilWorker *pointerWorker
	var nilMode *pointerMode

	tests := []struct {
		name   string
		ctx    context.Context
		worker Worker
		mode   Mode
		config Config
		target error
	}{
		{
			name:   "nil context",
			worker: validWorker,
			mode:   validMode,
			config: validConfig,
			target: ErrNilContext,
		},
		{
			name:   "empty name",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   validMode,
			config: validConfig,
			target: ErrEmptyName,
		},
		{
			name:   "name with whitespace",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   validMode,
			config: validConfig,
			target: ErrInvalidWorkerName,
		},
		{
			name:   "name with uppercase",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   validMode,
			config: validConfig,
			target: ErrInvalidWorkerName,
		},
		{
			name:   "name with hyphen",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   validMode,
			config: validConfig,
			target: ErrInvalidWorkerName,
		},
		{
			name:   "name with non ASCII letters",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   validMode,
			config: validConfig,
			target: ErrInvalidWorkerName,
		},
		{
			name:   "nil worker",
			ctx:    context.Background(),
			mode:   validMode,
			config: validConfig,
			target: ErrNilWorker,
		},
		{
			name:   "typed nil worker",
			ctx:    context.Background(),
			worker: nilWorker,
			mode:   validMode,
			config: validConfig,
			target: ErrNilWorker,
		},
		{
			name:   "nil mode",
			ctx:    context.Background(),
			worker: validWorker,
			config: validConfig,
			target: ErrNilMode,
		},
		{
			name:   "typed nil mode",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   nilMode,
			config: validConfig,
			target: ErrNilMode,
		},
		{
			name:   "negative timeout",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   validMode,
			config: Config{
				Timeout: -time.Second,
				Logger:  &recordingLogger{},
			},
			target: ErrInvalidTimeout,
		},
		{
			name:   "invalid retry",
			ctx:    context.Background(),
			worker: validWorker,
			mode:   validMode,
			config: Config{
				Retry: Retry{
					MaxAttempts: -1,
				},
				Logger: &recordingLogger{},
			},
			target: ErrInvalidRetryAttempts,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workerName := "valid_worker_2"
			switch test.name {
			case "empty name":
				workerName = ""
			case "name with whitespace":
				workerName = " "
			case "name with uppercase":
				workerName = "Worker"
			case "name with hyphen":
				workerName = "worker-name"
			case "name with non ASCII letters":
				workerName = "воркер"
			}
			result, err := Start(
				test.ctx,
				workerName,
				test.worker,
				test.mode,
				test.config,
			)
			if !errors.Is(err, test.target) {
				t.Fatalf("Start() error = %v, want %v", err, test.target)
			}
			if result != nil {
				t.Fatal("Start() result must be nil after validation failure")
			}
		})
	}
}

type pointerWorker struct{}

func (*pointerWorker) Execute(context.Context) error {
	return nil
}

type pointerMode struct{}

func (*pointerMode) validate() error {
	return nil
}

func (*pointerMode) run(
	context.Context,
	string,
	Logger,
	func(context.Context) error,
) error {
	return nil
}

type validationMode struct {
	validateFn func() error
	runFn      func(context.Context, func(context.Context) error) error
}

func (m validationMode) validate() error {
	return m.validateFn()
}

func (m validationMode) run(
	ctx context.Context,
	_ string,
	_ Logger,
	execute func(context.Context) error,
) error {
	return m.runFn(ctx, execute)
}
