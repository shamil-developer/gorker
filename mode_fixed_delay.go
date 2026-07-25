package gorker

import (
	"context"
	"errors"
	"time"
)

// FixedDelay waits Delay after an execution finishes before starting the next.
// Executions never overlap and missed runs never accumulate.
type FixedDelay struct {
	Delay     time.Duration
	Immediate bool
}

func (f FixedDelay) validate() error {
	if f.Delay <= 0 {
		return errInvalidDelay
	}
	return nil
}

func (f FixedDelay) run(
	ctx context.Context,
	name string,
	logger Logger,
	execute func(context.Context) error,
) error {
	if f.Immediate {
		logger.Debug(
			"worker fixed-delay execution triggered",
			"worker", name,
			"mode", "fixed_delay",
			"trigger", "immediate",
		)
		if err := execute(ctx); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return err
			}
			logger.Warn(
				"worker fixed-delay execution failed; continuing",
				"worker", name,
				"mode", "fixed_delay",
				"error", err,
				"next_in", f.Delay,
			)
		}
	}

	for {
		logger.Debug(
			"worker waiting for fixed-delay execution",
			"worker", name,
			"mode", "fixed_delay",
			"wait_for", f.Delay,
			"next_at", time.Now().Add(f.Delay),
		)
		if err := waitDuration(ctx, f.Delay); err != nil {
			return err
		}
		logger.Debug(
			"worker fixed-delay execution triggered",
			"worker", name,
			"mode", "fixed_delay",
		)
		if err := execute(ctx); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return err
			}
			logger.Warn(
				"worker fixed-delay execution failed; continuing",
				"worker", name,
				"mode", "fixed_delay",
				"error", err,
				"next_in", f.Delay,
			)
		}
	}
}
