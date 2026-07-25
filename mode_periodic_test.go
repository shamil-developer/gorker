package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestPeriodicExecutesOnEveryTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0

		err := (Periodic{Interval: time.Minute}).run(
			ctx,
			"periodic",
			&recordingLogger{},
			func(context.Context) error {
				calls++
				if calls == 3 {
					cancel()
				}
				return nil
			},
		)

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Periodic.run() error = %v, want Canceled", err)
		}
		if calls != 3 {
			t.Fatalf("calls = %d, want 3", calls)
		}
	})
}

func TestPeriodicImmediate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	err := (Periodic{
		Interval:  time.Hour,
		Immediate: true,
	}).run(ctx, "periodic", &recordingLogger{}, func(context.Context) error {
		calls++
		cancel()
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Periodic.run() error = %v, want Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 immediate call", calls)
	}
}

func TestPeriodicDoesNotOverlapExecutions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		started := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		calls := 0

		go func() {
			done <- (Periodic{
				Interval:  time.Second,
				Immediate: true,
			}).run(ctx, "periodic", &recordingLogger{}, func(context.Context) error {
				calls++
				close(started)
				<-release
				return nil
			})
		}()

		<-started
		time.Sleep(10 * time.Second)
		if calls != 1 {
			t.Fatalf("calls while first execution blocked = %d, want 1", calls)
		}

		cancel()
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Periodic.run() error = %v, want Canceled", err)
		}
	})
}

func TestPeriodicLogsSkippedExecutions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		logger := &recordingLogger{}
		calls := 0

		err := (Periodic{Interval: time.Second}).run(
			ctx,
			"periodic",
			logger,
			func(context.Context) error {
				calls++
				if calls == 1 {
					time.Sleep(5 * time.Second)
				} else {
					cancel()
				}
				return nil
			},
		)

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Periodic.run() error = %v, want Canceled", err)
		}

		for _, entry := range logger.snapshot() {
			if entry.level == "warn" &&
				entry.message == "worker periodic executions skipped" {
				return
			}
		}
		t.Fatal("missing warning for skipped periodic executions")
	})
}

func TestPeriodicContinuesAfterExecutionError(t *testing.T) {
	wantErr := errors.New("failure")

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0

		err := (Periodic{
			Interval:  time.Second,
			Immediate: true,
		}).run(ctx, "periodic", &recordingLogger{}, func(context.Context) error {
			calls++
			if calls == 1 {
				return wantErr
			}
			cancel()
			return nil
		})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Periodic.run() error = %v, want Canceled", err)
		}
		if calls != 2 {
			t.Fatalf("calls = %d, want 2", calls)
		}
	})
}

func TestPeriodicCanceledExecutionStopsMode(t *testing.T) {
	calls := 0
	err := (Periodic{
		Interval:  time.Hour,
		Immediate: true,
	}).run(
		context.Background(),
		"periodic",
		&recordingLogger{},
		func(context.Context) error {
			calls++
			return context.Canceled
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Periodic.run() error = %v, want Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestPeriodicValidation(t *testing.T) {
	if err := (Periodic{}).validate(); !errors.Is(err, ErrInvalidInterval) {
		t.Fatalf("zero interval error = %v, want ErrInvalidInterval", err)
	}
	if err := (Periodic{Interval: time.Second}).validate(); err != nil {
		t.Fatalf("valid Periodic.validate() error = %v", err)
	}
}
