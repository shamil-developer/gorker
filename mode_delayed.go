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
		return errInvalidDelay
	}
	return nil
}

func (d Delayed) run(
	ctx context.Context,
	name string,
	logger Logger,
	execute func(context.Context) error,
) error {
	logger.Debug(
		"worker waiting for delayed execution",
		"worker", name,
		"mode", "delayed",
		"wait_for", d.Delay,
		"next_at", time.Now().Add(d.Delay),
	)
	if err := waitDuration(ctx, d.Delay); err != nil {
		return err
	}

	logger.Debug(
		"worker delayed execution triggered",
		"worker", name,
		"mode", "delayed",
	)
	return execute(ctx)
}
