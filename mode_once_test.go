package gorker

import (
	"context"
	"errors"
	"testing"
)

func TestOnceExecutesExactlyOnce(t *testing.T) {
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	calls := 0

	err := (Once{}).run(ctx, func(got context.Context) error {
		calls++
		if got != ctx {
			t.Fatal("Once passed a different context")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Once.run() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestOncePropagatesError(t *testing.T) {
	wantErr := errors.New("failure")
	err := (Once{}).run(context.Background(), func(context.Context) error {
		return wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("Once.run() error = %v, want worker error", err)
	}
}

func TestOnceValidation(t *testing.T) {
	if err := (Once{}).validate(); err != nil {
		t.Fatalf("Once.validate() error = %v", err)
	}
}

type contextKey struct{}
