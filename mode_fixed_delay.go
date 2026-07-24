package gorker

import (
	"context"
	"time"
)

// FixedDelay waits Delay after an execution finishes before starting the next.
// Executions never overlap and missed runs never accumulate.
type FixedDelay struct {
	Delay       time.Duration
	Immediate   bool
	StopOnError bool
}

func (f FixedDelay) validate() error {
	if f.Delay <= 0 {
		return ErrInvalidDelay
	}
	return nil
}

func (f FixedDelay) run(
	ctx context.Context,
	execute func(context.Context) error,
) error {
	if f.Immediate {
		if err := execute(ctx); err != nil && f.StopOnError {
			return err
		}
	}

	for {
		if err := waitDuration(ctx, f.Delay); err != nil {
			return err
		}
		if err := execute(ctx); err != nil && f.StopOnError {
			return err
		}
	}
}
