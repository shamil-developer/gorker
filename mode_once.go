package gorker

import "context"

// Once executes a worker exactly once.
type Once struct{}

func (Once) validate() error {
	return nil
}

func (Once) run(
	ctx context.Context,
	execute func(context.Context) error,
) error {
	return execute(ctx)
}
