package gorker

import (
	"fmt"
	"time"
)

// Retry configures repeated attempts with an exponentially increasing delay.
//
// A zero Retry disables repeated attempts. Setting InitialDelay and MaxDelay
// to the same value produces a constant delay.
type Retry struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
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
		return ErrInvalidRetryAttempts
	}
	if r.MaxAttempts == 1 {
		if r.InitialDelay != 0 || r.MaxDelay != 0 {
			return ErrInvalidRetryAttempts
		}
		return nil
	}
	if r.InitialDelay <= 0 {
		return fmt.Errorf("%w: initial delay must be positive", ErrInvalidRetryDelay)
	}
	if r.MaxDelay <= 0 {
		return fmt.Errorf("%w: maximum delay must be positive", ErrInvalidRetryDelay)
	}
	if r.MaxDelay < r.InitialDelay {
		return fmt.Errorf("%w: maximum delay must not be less than initial delay", ErrInvalidRetryDelay)
	}
	return nil
}
