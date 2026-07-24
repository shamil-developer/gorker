package gorker

import (
	"context"
	"time"
)

// Periodic executes a worker on every interval until context cancellation.
type Periodic struct {
	Interval  time.Duration
	Immediate bool
	// StopOnError stops the mode when a worker exhausts all retry attempts.
	// By default Periodic logs the error and continues on the next interval.
	StopOnError bool
}

func (p Periodic) validate() error {
	if p.Interval <= 0 {
		return ErrInvalidInterval
	}
	return nil
}

func (p Periodic) run(
	ctx context.Context,
	execute func(context.Context) error,
) error {
	if p.Immediate {
		if err := execute(ctx); err != nil {
			if p.StopOnError {
				return err
			}
		}
	}

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := execute(ctx); err != nil {
				if p.StopOnError {
					return err
				}
			}
		}
	}
}
