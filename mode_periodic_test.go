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
	}).run(ctx, func(context.Context) error {
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
			}).run(ctx, func(context.Context) error {
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

func TestPeriodicErrorPolicy(t *testing.T) {
	wantErr := errors.New("failure")

	t.Run("continue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			calls := 0

			err := (Periodic{
				Interval:  time.Second,
				Immediate: true,
			}).run(ctx, func(context.Context) error {
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
	})

	t.Run("stop", func(t *testing.T) {
		calls := 0
		err := (Periodic{
			Interval:    time.Hour,
			Immediate:   true,
			StopOnError: true,
		}).run(context.Background(), func(context.Context) error {
			calls++
			return wantErr
		})

		if !errors.Is(err, wantErr) {
			t.Fatalf("Periodic.run() error = %v, want worker error", err)
		}
		if calls != 1 {
			t.Fatalf("calls = %d, want 1", calls)
		}
	})
}

func TestPeriodicValidation(t *testing.T) {
	if err := (Periodic{}).validate(); !errors.Is(err, ErrInvalidInterval) {
		t.Fatalf("zero interval error = %v, want ErrInvalidInterval", err)
	}
	if err := (Periodic{Interval: time.Second}).validate(); err != nil {
		t.Fatalf("valid Periodic.validate() error = %v", err)
	}
}
