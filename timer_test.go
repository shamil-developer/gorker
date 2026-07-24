package gorker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestWaitDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		if err := waitDuration(t.Context(), 10*time.Second); err != nil {
			t.Fatalf("waitDuration() error = %v", err)
		}
		if elapsed := time.Since(start); elapsed != 10*time.Second {
			t.Fatalf("elapsed = %v, want 10s", elapsed)
		}
	})
}

func TestWaitDurationReturnsImmediatelyForZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		if err := waitDuration(t.Context(), 0); err != nil {
			t.Fatalf("waitDuration() error = %v", err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("elapsed = %v, want 0", elapsed)
		}
	})
}

func TestWaitDurationHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := waitDuration(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitDuration() error = %v, want Canceled", err)
	}
}
