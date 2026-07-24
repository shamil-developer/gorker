package gorker

import (
	"context"
	"time"
)

// Delayed executes a worker once after Delay.
type Delayed struct {
	Delay time.Duration
}

func (d Delayed) validate() error {
	if d.Delay <= 0 {
		return ErrInvalidDelay
	}
	return nil
}

func (d Delayed) run(
	ctx context.Context,
	execute func(context.Context) error,
) error {
	if err := waitDuration(ctx, d.Delay); err != nil {
		return err
	}

	return execute(ctx)
}
