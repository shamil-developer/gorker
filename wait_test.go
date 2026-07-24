package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestWaitAcceptsResultSlice(t *testing.T) {
	results := []Result{
		completedResult(nil),
		nil,
		completedResult(context.Canceled),
	}

	if err := Wait(context.Background(), results...); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestWaitJoinsWorkerErrors(t *testing.T) {
	firstErr := errors.New("first")
	secondErr := errors.New("second")

	err := Wait(
		context.Background(),
		completedResult(firstErr),
		completedResult(secondErr),
	)

	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Wait() error = %v, want both worker errors", err)
	}
}

func TestWaitStopsAtContextDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := make(chan error)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		err := Wait(ctx, Result(result))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait() error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestWaitRejectsNilContext(t *testing.T) {
	//lint:ignore SA1012 nil context is intentional to test public validation
	if err := Wait(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Wait() error = %v, want ErrNilContext", err)
	}
}

func completedResult(err error) Result {
	result := make(chan error, 1)
	if err != nil {
		result <- err
	}
	close(result)
	return result
}
