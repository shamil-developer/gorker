package gorker

import (
	"fmt"
	"time"
)

// Retry configures retries with an exponentially increasing delay.
//
// The zero value performs one attempt without retries. Setting InitialDelay and
// MaxDelay to the same value produces a constant delay. Recovered panics are
// not retried.
type Retry struct {
	// MaxAttempts is the total number of attempts, including the first.
	// Zero and one both mean a single attempt.
	MaxAttempts int

	// InitialDelay is the delay before the second attempt.
	InitialDelay time.Duration

	// MaxDelay caps the delay between attempts.
	MaxDelay time.Duration
}

func (r Retry) nextDelay(attempt int) time.Duration {
	if r.InitialDelay <= 0 || attempt <= 0 {
		return 0
	}

	delay := r.InitialDelay
	for current := 1; current < attempt; current++ {
		if delay >= r.MaxDelay || delay > r.MaxDelay/2 {
			return r.MaxDelay
		}
		delay *= 2
	}

	if delay > r.MaxDelay {
		return r.MaxDelay
	}
	return delay
}

func (r Retry) validate() error {
	if r == (Retry{}) {
		return nil
	}
	if r.MaxAttempts < 1 {
		return errInvalidRetryAttempts
	}
	if r.MaxAttempts == 1 {
		if r.InitialDelay != 0 || r.MaxDelay != 0 {
			return errInvalidRetryAttempts
		}
		return nil
	}
	if r.InitialDelay <= 0 {
		return fmt.Errorf("%w: initial delay must be positive", errInvalidRetryDelay)
	}
	if r.MaxDelay <= 0 {
		return fmt.Errorf("%w: maximum delay must be positive", errInvalidRetryDelay)
	}
	if r.MaxDelay < r.InitialDelay {
		return fmt.Errorf("%w: maximum delay must not be less than initial delay", errInvalidRetryDelay)
	}
	return nil
}
