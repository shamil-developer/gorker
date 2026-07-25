package gorker

import "context"

type Once struct{}

func (Once) validate() error {
	return nil
}

func (Once) run(
	ctx context.Context,
	name string,
	logger Logger,
	execute func(context.Context) error,
) error {
	logger.Debug(
		"worker once execution triggered",
		"worker", name,
		"mode", "once",
	)
	return execute(ctx)
}
