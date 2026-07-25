package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestDelayedExecutesAfterDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		calledAt := time.Time{}

		err := (Delayed{Delay: time.Minute}).run(
			t.Context(),
			"delayed",
			&recordingLogger{},
			func(context.Context) error {
				calledAt = time.Now()
				return nil
			},
		)

		if err != nil {
			t.Fatalf("Delayed.run() error = %v", err)
		}
		if elapsed := calledAt.Sub(start); elapsed != time.Minute {
			t.Fatalf("worker called after %v, want 1m", elapsed)
		}
	})
}

func TestDelayedCancellationPreventsExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		called := false
		done := make(chan error, 1)

		go func() {
			done <- (Delayed{Delay: time.Minute}).run(
				ctx,
				"delayed",
				&recordingLogger{},
				func(context.Context) error {
					called = true
					return nil
				},
			)
		}()

		time.Sleep(30 * time.Second)
		cancel()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Delayed.run() error = %v, want Canceled", err)
		}
		if called {
			t.Fatal("worker executed after context cancellation")
		}
	})
}

func TestDelayedRejectsInvalidDelay(t *testing.T) {
	for _, delay := range []time.Duration{0, -time.Second} {
		err := (Delayed{Delay: delay}).validate()
		if !errors.Is(err, ErrInvalidDelay) {
			t.Fatalf("Delay %v: error = %v, want ErrInvalidDelay", delay, err)
		}
	}
	if err := (Delayed{Delay: time.Second}).validate(); err != nil {
		t.Fatalf("valid Delayed.validate() error = %v", err)
	}
}
