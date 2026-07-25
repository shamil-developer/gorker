package gorker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	robfigcron "github.com/robfig/cron/v3"
)

// Cron executes a worker according to a standard five-field cron expression.
//
// Executions never overlap. Occurrences missed while an execution is running
// are skipped. Expressions use time.Local unless they include a TZ or CRON_TZ
// prefix.
type Cron struct {
	// Expression is the cron schedule and must not be empty.
	Expression string
}

func (c Cron) validate() error {
	_, _, err := parseCronSchedule(c.Expression, time.Now())
	return err
}

func (c Cron) run(
	ctx context.Context,
	name string,
	logger Logger,
	execute func(context.Context) error,
) error {
	schedule, next, err := parseCronSchedule(c.Expression, time.Now())
	if err != nil {
		return err
	}

	for {
		logger.Debug(
			"worker waiting for cron execution",
			"worker", name,
			"mode", "cron",
			"expression", c.Expression,
			"next_at", next,
			"wait_for", time.Until(next),
		)
		if err := waitDuration(ctx, time.Until(next)); err != nil {
			return err
		}
		logger.Debug(
			"worker cron execution triggered",
			"worker", name,
			"mode", "cron",
			"expression", c.Expression,
			"scheduled_at", next,
		)
		if err := execute(ctx); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return err
			}
			logger.Warn(
				"worker cron execution failed; continuing",
				"worker", name,
				"mode", "cron",
				"expression", c.Expression,
				"error", err,
			)
		}

		finishedAt := time.Now()
		firstSkippedAt := schedule.Next(next)
		next, err = nextCronActivation(schedule, finishedAt)
		if err != nil {
			return err
		}
		if !firstSkippedAt.IsZero() && !firstSkippedAt.After(finishedAt) {
			logger.Warn(
				"worker cron executions skipped",
				"worker", name,
				"mode", "cron",
				"expression", c.Expression,
				"first_skipped_at", firstSkippedAt,
				"next_at", next,
			)
		}
	}
}

func parseCronSchedule(
	expression string,
	now time.Time,
) (robfigcron.Schedule, time.Time, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, time.Time{}, errInvalidCronExpression
	}

	schedule, err := robfigcron.ParseStandard(expression)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: %w", errInvalidCronExpression, err)
	}

	next, err := nextCronActivation(schedule, now)
	if err != nil {
		return nil, time.Time{}, err
	}
	return schedule, next, nil
}

func nextCronActivation(
	schedule robfigcron.Schedule,
	now time.Time,
) (time.Time, error) {
	next := schedule.Next(now)
	if next.IsZero() {
		return time.Time{}, fmt.Errorf(
			"%w: schedule has no future activation",
			errInvalidCronExpression,
		)
	}
	return next, nil
}
