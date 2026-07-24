package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestScheduledExecutesAtTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		at := start.Add(time.Hour)
		calledAt := time.Time{}

		err := (Scheduled{At: at}).run(
			t.Context(),
			func(context.Context) error {
				calledAt = time.Now()
				return nil
			},
		)

		if err != nil {
			t.Fatalf("Scheduled.run() error = %v", err)
		}
		if !calledAt.Equal(at) {
			t.Fatalf("worker called at %v, want %v", calledAt, at)
		}
	})
}

func TestScheduledPastTimeExecutesImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		calledAt := time.Time{}

		err := (Scheduled{At: now.Add(-time.Hour)}).run(
			t.Context(),
			func(context.Context) error {
				calledAt = time.Now()
				return nil
			},
		)

		if err != nil {
			t.Fatalf("Scheduled.run() error = %v", err)
		}
		if !calledAt.Equal(now) {
			t.Fatalf("worker called at %v, want immediate execution at %v", calledAt, now)
		}
	})
}

func TestScheduledRejectsZeroTime(t *testing.T) {
	err := (Scheduled{}).validate()
	if !errors.Is(err, ErrInvalidScheduledTime) {
		t.Fatalf("Scheduled.validate() error = %v, want ErrInvalidScheduledTime", err)
	}
	if err := (Scheduled{At: time.Now()}).validate(); err != nil {
		t.Fatalf("valid Scheduled.validate() error = %v", err)
	}
}

func TestScheduledHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := (Scheduled{At: time.Now().Add(time.Hour)}).run(
		ctx,
		func(context.Context) error {
			t.Fatal("worker must not execute")
			return nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Scheduled.run() error = %v, want Canceled", err)
	}
}
