package gorker

import (
	"context"
	"fmt"
	"strings"
	"time"

	robfigcron "github.com/robfig/cron/v3"
)

// Cron executes a worker according to a standard five-field cron expression.
//
// Executions never overlap. Schedule occurrences missed while a worker is
// running are skipped; after completion, only the next future time is used.
// Expressions use time.Local unless they include a TZ or CRON_TZ prefix.
type Cron struct {
	Expression  string
	StopOnError bool
}

func (c Cron) validate() error {
	_, _, err := parseCronSchedule(c.Expression, time.Now())
	return err
}

func (c Cron) run(
	ctx context.Context,
	execute func(context.Context) error,
) error {
	schedule, next, err := parseCronSchedule(c.Expression, time.Now())
	if err != nil {
		return err
	}

	for {
		if err := waitDuration(ctx, time.Until(next)); err != nil {
			return err
		}
		if err := execute(ctx); err != nil && c.StopOnError {
			return err
		}

		next, err = nextCronActivation(schedule, time.Now())
		if err != nil {
			return err
		}
	}
}

func parseCronSchedule(
	expression string,
	now time.Time,
) (robfigcron.Schedule, time.Time, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, time.Time{}, ErrInvalidCronExpression
	}

	schedule, err := robfigcron.ParseStandard(expression)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: %w", ErrInvalidCronExpression, err)
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
			ErrInvalidCronExpression,
		)
	}
	return next, nil
}
