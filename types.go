package gorker

import (
	"context"
	"time"
)

// Worker contains only the business operation. Start controls its lifecycle.
type Worker interface {
	Execute(ctx context.Context) error
}

// Mode controls when a worker is executed.
//
// Only the built-in modes in this package implement Mode.
type Mode interface {
	validate() error
	run(
		ctx context.Context,
		execute func(context.Context) error,
	) error
}

type Config struct {
	// Timeout limits each individual attempt. Zero disables it.
	Timeout time.Duration

	// Retry configures repeated attempts. Its zero value means one attempt.
	Retry Retry

	// Logger receives structured worker lifecycle events.
	// It is required.
	Logger Logger
}
