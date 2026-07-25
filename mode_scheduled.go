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
		return errInvalidScheduledTime
	}
	return nil
}

func (s Scheduled) run(
	ctx context.Context,
	name string,
	logger Logger,
	execute func(context.Context) error,
) error {
	waitFor := time.Until(s.At)
	logger.Debug(
		"worker waiting for scheduled execution",
		"worker", name,
		"mode", "scheduled",
		"scheduled_at", s.At,
		"wait_for", max(waitFor, 0),
		"immediate", waitFor <= 0,
	)
	if err := waitDuration(ctx, waitFor); err != nil {
		return err
	}

	logger.Debug(
		"worker scheduled execution triggered",
		"worker", name,
		"mode", "scheduled",
		"scheduled_at", s.At,
	)
	return execute(ctx)
}
