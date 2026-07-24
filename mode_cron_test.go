package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestCronRunsOnSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now()
		var calls []time.Time

		err := (Cron{Expression: "@every 1s"}).run(
			ctx,
			func(context.Context) error {
				calls = append(calls, time.Now())
				if len(calls) == 2 {
					cancel()
				}
				return nil
			},
		)

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Cron.run() error = %v, want Canceled", err)
		}
		if len(calls) != 2 {
			t.Fatalf("calls = %d, want 2", len(calls))
		}
		if elapsed := calls[0].Sub(start); elapsed != time.Second {
			t.Fatalf("first call after %v, want 1s", elapsed)
		}
	})
}

func TestCronExpressionTimezone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now()
		calledAt := time.Time{}

		err := (Cron{
			Expression: "CRON_TZ=Etc/GMT-2 0 1 * * *",
		}).run(ctx, func(context.Context) error {
			calledAt = time.Now()
			cancel()
			return nil
		})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Cron.run() error = %v, want Canceled", err)
		}
		if elapsed := calledAt.Sub(start); elapsed != 23*time.Hour {
			t.Fatalf("worker called after %v, want 23h for UTC+2 schedule", elapsed)
		}
	})
}

func TestCronSkipsMissedExecutions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		secondStarted := make(chan struct{})
		done := make(chan error, 1)
		var starts []time.Time

		go func() {
			done <- (Cron{Expression: "@every 1s"}).run(
				ctx,
				func(context.Context) error {
					starts = append(starts, time.Now())
					if len(starts) == 1 {
						close(firstStarted)
						<-releaseFirst
					} else {
						close(secondStarted)
						cancel()
					}
					return nil
				},
			)
		}()

		<-firstStarted
		time.Sleep(5 * time.Second)
		if len(starts) != 1 {
			t.Fatalf("calls while first execution blocked = %d, want 1", len(starts))
		}

		close(releaseFirst)
		<-secondStarted
		if gap := starts[1].Sub(starts[0]); gap != 6*time.Second {
			t.Fatalf("start gap = %v, want missed runs skipped and next run after 6s", gap)
		}
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Cron.run() error = %v, want Canceled", err)
		}
	})
}

func TestCronErrorPolicy(t *testing.T) {
	wantErr := errors.New("failure")

	t.Run("continue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			calls := 0

			err := (Cron{Expression: "@every 1s"}).run(
				ctx,
				func(context.Context) error {
					calls++
					if calls == 1 {
						return wantErr
					}
					cancel()
					return nil
				},
			)

			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Cron.run() error = %v, want Canceled", err)
			}
		})
	})

	t.Run("stop", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			err := (Cron{
				Expression:  "@every 1s",
				StopOnError: true,
			}).run(t.Context(), func(context.Context) error {
				return wantErr
			})
			if !errors.Is(err, wantErr) {
				t.Fatalf("Cron.run() error = %v, want worker error", err)
			}
		})
	})
}

func TestCronValidation(t *testing.T) {
	for _, expression := range []string{
		"",
		"not a cron expression",
		"0 0 31 2 *",
	} {
		err := (Cron{Expression: expression}).validate()
		if !errors.Is(err, ErrInvalidCronExpression) {
			t.Fatalf("Expression %q: error = %v, want ErrInvalidCronExpression", expression, err)
		}
	}
	if err := (Cron{Expression: "@every 1s"}).validate(); err != nil {
		t.Fatalf("valid Cron.validate() error = %v", err)
	}
}
