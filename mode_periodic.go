package gorker

import (
	"context"
	"errors"
	"time"
)

// Periodic executes a worker on every interval until context cancellation.
type Periodic struct {
	Interval  time.Duration
	Immediate bool
}

func (p Periodic) validate() error {
	if p.Interval <= 0 {
		return errInvalidInterval
	}
	return nil
}

func (p Periodic) run(
	ctx context.Context,
	name string,
	logger Logger,
	execute func(context.Context) error,
) error {
	if p.Immediate {
		logger.Debug(
			"worker periodic execution triggered",
			"worker", name,
			"mode", "periodic",
			"trigger", "immediate",
		)
		if err := execute(ctx); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return err
			}
			logger.Warn(
				"worker periodic execution failed; continuing",
				"worker", name,
				"mode", "periodic",
				"error", err,
				"next_in", p.Interval,
			)
		}
	}

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	logger.Debug(
		"worker waiting for periodic execution",
		"worker", name,
		"mode", "periodic",
		"interval", p.Interval,
		"next_at", time.Now().Add(p.Interval),
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case scheduledAt := <-ticker.C:
			lag := time.Since(scheduledAt)
			if lag >= p.Interval {
				logger.Warn(
					"worker periodic executions skipped",
					"worker", name,
					"mode", "periodic",
					"interval", p.Interval,
					"scheduled_at", scheduledAt,
					"lag", lag,
				)
			}
			logger.Debug(
				"worker periodic execution triggered",
				"worker", name,
				"mode", "periodic",
				"scheduled_at", scheduledAt,
			)
			if err := execute(ctx); err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return err
				}
				logger.Warn(
					"worker periodic execution failed; continuing",
					"worker", name,
					"mode", "periodic",
					"error", err,
					"next_in", p.Interval,
				)
			}
		}
	}
}
