package gorker

import (
	"errors"
	"testing"
	"time"
)

func TestRetryIncreasesDelayUpToMaximum(t *testing.T) {
	retry := Retry{
		MaxAttempts:  8,
		InitialDelay: time.Second,
		MaxDelay:     30 * time.Second,
	}
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}

	for index, expected := range want {
		attempt := index + 1
		if got := retry.nextDelay(attempt); got != expected {
			t.Fatalf("nextDelay(%d) = %v, want %v", attempt, got, expected)
		}
	}
}

func TestRetryEqualDelaysProduceConstantDelay(t *testing.T) {
	retry := Retry{
		MaxAttempts:  4,
		InitialDelay: 3 * time.Second,
		MaxDelay:     3 * time.Second,
	}

	for attempt := 1; attempt < retry.MaxAttempts; attempt++ {
		if got := retry.nextDelay(attempt); got != 3*time.Second {
			t.Fatalf("nextDelay(%d) = %v, want 3s", attempt, got)
		}
	}
}

func TestRetryValidation(t *testing.T) {
	tests := []struct {
		name   string
		retry  Retry
		target error
	}{
		{
			name: "zero value disables retry",
		},
		{
			name: "one attempt without retry settings",
			retry: Retry{
				MaxAttempts: 1,
			},
		},
		{
			name: "rejects negative attempts",
			retry: Retry{
				MaxAttempts: -1,
			},
			target: errInvalidRetryAttempts,
		},
		{
			name: "retry settings require two attempts",
			retry: Retry{
				MaxAttempts:  1,
				InitialDelay: time.Second,
				MaxDelay:     time.Second,
			},
			target: errInvalidRetryAttempts,
		},
		{
			name: "delays without attempts",
			retry: Retry{
				InitialDelay: time.Second,
				MaxDelay:     time.Second,
			},
			target: errInvalidRetryAttempts,
		},
		{
			name: "requires positive initial delay",
			retry: Retry{
				MaxAttempts: 2,
				MaxDelay:    time.Second,
			},
			target: errInvalidRetryDelay,
		},
		{
			name: "rejects negative initial delay",
			retry: Retry{
				MaxAttempts:  2,
				InitialDelay: -time.Second,
				MaxDelay:     time.Second,
			},
			target: errInvalidRetryDelay,
		},
		{
			name: "requires positive maximum delay",
			retry: Retry{
				MaxAttempts:  2,
				InitialDelay: time.Second,
			},
			target: errInvalidRetryDelay,
		},
		{
			name: "maximum cannot be below initial",
			retry: Retry{
				MaxAttempts:  2,
				InitialDelay: 2 * time.Second,
				MaxDelay:     time.Second,
			},
			target: errInvalidRetryDelay,
		},
		{
			name: "valid increasing delay",
			retry: Retry{
				MaxAttempts:  2,
				InitialDelay: time.Second,
				MaxDelay:     2 * time.Second,
			},
		},
		{
			name: "valid constant delay",
			retry: Retry{
				MaxAttempts:  2,
				InitialDelay: time.Second,
				MaxDelay:     time.Second,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.retry.validate()
			if !errors.Is(err, test.target) {
				t.Fatalf("validate() error = %v, want %v", err, test.target)
			}
		})
	}
}

func TestRetryDelayDoesNotOverflow(t *testing.T) {
	retry := Retry{
		MaxAttempts:  64,
		InitialDelay: time.Duration(1 << 62),
		MaxDelay:     time.Duration(1<<63 - 1),
	}

	if got := retry.nextDelay(2); got != time.Duration(1<<63-1) {
		t.Fatalf("nextDelay(2) = %v, want maximum duration", got)
	}
}

func TestRetryReturnsZeroForInvalidAttempt(t *testing.T) {
	retry := Retry{
		InitialDelay: time.Second,
		MaxDelay:     time.Minute,
	}
	if got := retry.nextDelay(0); got != 0 {
		t.Fatalf("nextDelay(0) = %v, want 0", got)
	}
}
