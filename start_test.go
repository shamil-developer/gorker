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
		result := Start(
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
	result := Start(
		context.Background(),
		"failure",
		workerFunc(func(context.Context) error { return wantErr }),
		Once{},
		Config{Logger: logger},
	)

	if err, ok := <-result; !ok || !errors.Is(err, wantErr) {
		t.Fatalf("first receive = (%v, %v), want worker error", err, ok)
	}
	if err, ok := <-result; ok || err != nil {
		t.Fatalf("second receive = (%v, %v), want closed channel", err, ok)
	}

	entries := logger.snapshot()
	lastEntry := entries[len(entries)-1]
	if event, _ := lastEntry.value("event"); event != eventWorkerFailed {
		t.Fatalf("last event = %v, want %q", event, eventWorkerFailed)
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
			result := Start(
				context.Background(),
				"required_logger",
				workerFunc(func(context.Context) error {
					calls++
					return nil
				}),
				Once{},
				Config{Logger: test.logger},
			)

			if err := <-result; !errors.Is(err, ErrNilLogger) {
				t.Fatalf("Start() error = %v, want ErrNilLogger", err)
			}
			if calls != 0 {
				t.Fatalf("worker calls = %d, want 0", calls)
			}
		})
	}
}

func TestStartValidatesModeBeforeStarting(t *testing.T) {
	logger := &recordingLogger{}
	wantErr := errors.New("invalid mode")
	runCalls := 0
	result := Start(
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

	if err := <-result; !errors.Is(err, wantErr) {
		t.Fatalf("Start() error = %v, want validation error", err)
	}
	if runCalls != 0 {
		t.Fatalf("Mode.run() calls = %d, want 0", runCalls)
	}

	entries := logger.snapshot()
	if len(entries) != 1 {
		t.Fatalf("log entries = %#v, want one rejection entry", entries)
	}
	if event, _ := entries[0].value("event"); event != eventWorkerStartRejected {
		t.Fatalf("event = %v, want %q", event, eventWorkerStartRejected)
	}
}

func TestStartRecoversModeValidationPanic(t *testing.T) {
	logger := &recordingLogger{}
	result := Start(
		context.Background(),
		"validation_panic",
		workerFunc(func(context.Context) error {
			t.Fatal("worker executed after mode validation panic")
			return nil
		}),
		validationMode{
			validateFn: func() error {
				panic("validation boom")
			},
			runFn: func(context.Context, func(context.Context) error) error {
				t.Fatal("Mode.run executed after validation panic")
				return nil
			},
		},
		Config{Logger: logger},
	)

	var panicErr *PanicError
	if err := <-result; !errors.As(err, &panicErr) {
		t.Fatalf("Start() error = %T %v, want *PanicError", err, err)
	}
	if len(panicErr.Stack) == 0 {
		t.Fatal("PanicError.Stack is empty")
	}

	entries := logger.snapshot()
	if len(entries) != 1 {
		t.Fatalf("log entries = %#v, want one panic entry", entries)
	}
	if event, _ := entries[0].value("event"); event != eventModeValidationPanic {
		t.Fatalf("event = %v, want %q", event, eventModeValidationPanic)
	}
	if _, exists := entries[0].value("stack"); !exists {
		t.Fatal("validation panic log has no stack")
	}
}

func TestStartLogsStructuredWorkerLifecycle(t *testing.T) {
	logger := &recordingLogger{}
	result := Start(
		context.Background(),
		"lifecycle",
		workerFunc(func(context.Context) error { return nil }),
		&Once{},
		Config{
			Timeout: time.Second,
			Logger:  logger,
		},
	)

	if err := <-result; err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	entries := logger.snapshot()
	if len(entries) != 4 {
		t.Fatalf("log entries = %#v, want four lifecycle entries", entries)
	}

	want := []struct {
		level string
		event string
	}{
		{"info", eventWorkerStarted},
		{"debug", eventAttemptStarted},
		{"info", eventAttemptCompleted},
		{"info", eventWorkerCompleted},
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
		if event, _ := entries[index].value("event"); event != expected.event {
			t.Errorf("entry %d event = %v, want %q", index, event, expected.event)
		}
	}

	if mode, _ := entries[0].value("mode"); mode != "gorker.Once" {
		t.Errorf("worker mode = %v, want gorker.Once", mode)
	}
	if timeout, _ := entries[0].value("timeout"); timeout != time.Second {
		t.Errorf("worker timeout = %v, want 1s", timeout)
	}
	if maxAttempts, _ := entries[0].value("max_attempts"); maxAttempts != 1 {
		t.Errorf("worker max_attempts = %v, want 1", maxAttempts)
	}
	if _, exists := entries[3].value("duration"); !exists {
		t.Fatal("worker completion entry has no duration")
	}
}

func TestStartLogsModeFailure(t *testing.T) {
	logger := &recordingLogger{}
	wantErr := errors.New("mode failed")
	result := Start(
		context.Background(),
		"mode_failure",
		workerFunc(func(context.Context) error { return nil }),
		modeFunc(func(context.Context, func(context.Context) error) error {
			return wantErr
		}),
		Config{Logger: logger},
	)

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
	if event, _ := entries[1].value("event"); event != eventWorkerFailed {
		t.Fatalf("failure event = %v, want %q", event, eventWorkerFailed)
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

	result := Start(
		ctx,
		"deadline",
		workerFunc(func(context.Context) error {
			t.Fatal("worker executed after parent deadline")
			return nil
		}),
		Once{},
		Config{Logger: logger},
	)

	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want DeadlineExceeded", err)
	}

	entries := logger.snapshot()
	lastEntry := entries[len(entries)-1]
	if event, _ := lastEntry.value("event"); event != eventWorkerStopped {
		t.Fatalf("last event = %v, want %q", event, eventWorkerStopped)
	}
	if reason, _ := lastEntry.value("reason"); reason != "deadline_exceeded" {
		t.Fatalf("stop reason = %v, want deadline_exceeded", reason)
	}
}

func TestStartRecoversModePanic(t *testing.T) {
	result := Start(
		context.Background(),
		"panic_mode",
		workerFunc(func(context.Context) error { return nil }),
		modeFunc(func(context.Context, func(context.Context) error) error {
			panic("mode boom")
		}),
		Config{Logger: &recordingLogger{}},
	)

	var panicErr *PanicError
	if err := <-result; !errors.As(err, &panicErr) {
		t.Fatalf("Start() error = %T %v, want *PanicError", err, err)
	}
}

func TestStartTreatsModeCancellationAsNormalLifecycle(t *testing.T) {
	logger := &recordingLogger{}
	result := Start(
		context.Background(),
		"canceled_mode",
		workerFunc(func(context.Context) error { return nil }),
		modeFunc(func(context.Context, func(context.Context) error) error {
			return context.Canceled
		}),
		Config{Logger: logger},
	)

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
					MaxAttempts: 1,
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
			result := Start(test.ctx, workerName, test.worker, test.mode, test.config)
			err := <-result
			if !errors.Is(err, test.target) {
				t.Fatalf("Start() error = %v, want %v", err, test.target)
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

func (*pointerMode) run(context.Context, func(context.Context) error) error {
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
	execute func(context.Context) error,
) error {
	return m.runFn(ctx, execute)
}
