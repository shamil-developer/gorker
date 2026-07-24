package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestFixedDelayStartsDelayAfterCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var starts []time.Time

		err := (FixedDelay{
			Delay:     2 * time.Second,
			Immediate: true,
		}).run(ctx, func(context.Context) error {
			starts = append(starts, time.Now())
			time.Sleep(5 * time.Second)
			if len(starts) == 2 {
				cancel()
			}
			return nil
		})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("FixedDelay.run() error = %v, want Canceled", err)
		}
		if len(starts) != 2 {
			t.Fatalf("starts = %d, want 2", len(starts))
		}
		if gap := starts[1].Sub(starts[0]); gap != 7*time.Second {
			t.Fatalf("start gap = %v, want execution 5s + delay 2s", gap)
		}
	})
}

func TestFixedDelayWaitsBeforeFirstRunByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx, cancel := context.WithCancel(t.Context())
		calledAt := time.Time{}

		err := (FixedDelay{Delay: time.Minute}).run(
			ctx,
			func(context.Context) error {
				calledAt = time.Now()
				cancel()
				return nil
			},
		)

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("FixedDelay.run() error = %v, want Canceled", err)
		}
		if elapsed := calledAt.Sub(start); elapsed != time.Minute {
			t.Fatalf("first run after %v, want 1m", elapsed)
		}
	})
}

func TestFixedDelayErrorPolicy(t *testing.T) {
	wantErr := errors.New("failure")

	t.Run("continue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			calls := 0

			err := (FixedDelay{
				Delay:     time.Second,
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
				t.Fatalf("FixedDelay.run() error = %v, want Canceled", err)
			}
		})
	})

	t.Run("stop", func(t *testing.T) {
		err := (FixedDelay{
			Delay:       time.Second,
			Immediate:   true,
			StopOnError: true,
		}).run(context.Background(), func(context.Context) error {
			return wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("FixedDelay.run() error = %v, want worker error", err)
		}
	})
}

func TestFixedDelayRejectsInvalidDelay(t *testing.T) {
	for _, delay := range []time.Duration{0, -time.Second} {
		err := (FixedDelay{Delay: delay}).validate()
		if !errors.Is(err, ErrInvalidDelay) {
			t.Fatalf("Delay %v: error = %v, want ErrInvalidDelay", delay, err)
		}
	}
	if err := (FixedDelay{Delay: time.Second}).validate(); err != nil {
		t.Fatalf("valid FixedDelay.validate() error = %v", err)
	}
}
