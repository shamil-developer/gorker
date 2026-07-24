package gorker

import (
	"context"
	"time"
)

// Scheduled executes a worker once at At.
// A time in the past executes immediately.
type Scheduled struct {
	At time.Time
}

func (s Scheduled) validate() error {
	if s.At.IsZero() {
		return ErrInvalidScheduledTime
	}
	return nil
}

func (s Scheduled) run(
	ctx context.Context,
	execute func(context.Context) error,
) error {
	if err := waitDuration(ctx, time.Until(s.At)); err != nil {
		return err
	}

	return execute(ctx)
}
