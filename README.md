# gorker

`gorker` is a small Go library for running background workers with scheduling,
per-attempt timeouts, retries, structured logging, panic recovery, and graceful
shutdown.

The library separates three concerns:

- `Worker` contains the business operation.
- `Mode` controls when the operation runs.
- `Config` controls how each execution is handled.

## Requirements

Go 1.25 or newer.

## Installation

```sh
go get github.com/shamil-developer/gorker
```

## Quick start

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shamil-developer/gorker"
)

type cleanupWorker struct{}

func (cleanupWorker) Execute(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	result, err := gorker.Start(
		ctx,
		"cleanup",
		cleanupWorker{},
		gorker.Periodic{
			Interval:  time.Minute,
			Immediate: true,
		},
		gorker.Config{
			Timeout: 10 * time.Second,
			Retry: gorker.Retry{
				MaxAttempts:  3,
				InitialDelay: time.Second,
				MaxDelay:     30 * time.Second,
			},
			Logger: slog.Default(),
		},
	)
	if err != nil {
		slog.Error("worker was not started", "error", err)
		return
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := gorker.Wait(shutdownCtx, result); err != nil {
		slog.Error("worker shutdown failed", "error", err)
	}
}
```

`Start` validates all arguments before creating a goroutine. A worker name must
contain only lowercase ASCII letters, digits, and underscores. Validation
failures are returned directly from `Start`.

After a successful start, runtime failures are reported through `Result`.
Successful completion closes the result channel without a value. A runtime
failure sends one error and then closes the channel.

## Modes

| Mode | Behavior |
| --- | --- |
| `Once` | Runs once immediately |
| `Delayed` | Runs once after `Delay` |
| `Scheduled` | Runs once at `At`; a past time runs immediately |
| `Periodic` | Runs on a fixed `Interval` |
| `FixedDelay` | Waits `Delay` after completion before the next run |
| `Cron` | Runs from a standard five-field cron expression |

Built-in modes never execute the same worker concurrently. `Periodic` does not
accumulate missed ticks, `FixedDelay` starts its delay after execution
completes, and `Cron` skips occurrences missed by a running execution.

`Periodic` and `FixedDelay` wait before their first execution by default. Set
`Immediate` to `true` to run once immediately.

`Cron` uses the process-local time zone. A different zone can be selected in
the expression:

```go
gorker.Cron{
	Expression: "CRON_TZ=Europe/Moscow 0 3 * * *",
}
```

## Timeout

`Config.Timeout` applies independently to every attempt. Zero disables the
timeout. Timeout cancellation is cooperative: `Worker.Execute` must observe its
context and stop its work.

If a worker returns after its attempt context has expired, the context error
takes precedence over the worker's returned error.

## Retry

The zero value of `Retry` performs one attempt. `MaxAttempts` includes the
initial attempt, so both zero and one mean no retries.

With retries enabled, the delay doubles after each failed attempt and is capped
by `MaxDelay`:

```go
gorker.Retry{
	MaxAttempts:  5,
	InitialDelay: time.Second,
	MaxDelay:     8 * time.Second,
}
```

This configuration waits `1s`, `2s`, `4s`, and `8s`. Set `InitialDelay` and
`MaxDelay` to the same value for a constant delay.

Each activation of a recurring mode receives its own complete retry sequence.
After all attempts fail, the recurring mode continues with its next scheduled
activation. Canceling the parent context stops retries immediately. Recovered
panics are not retried.

## Logging

`Config.Logger` is required. The standard library `*slog.Logger` implements the
interface:

```go
type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}
```

Log arguments are structured key-value pairs. The logger implementation must be
safe for concurrent use because separate workers may log concurrently.

## Shutdown

Use one parent context for related workers. During shutdown, cancel that
context and pass every result to `Wait`:

```go
if err := gorker.Wait(
	shutdownCtx,
	firstResult,
	secondResult,
	thirdResult,
); err != nil {
	slog.Error("worker shutdown failed", "error", err)
}
```

`Wait` waits for every result unless its own context expires. It ignores
`context.Canceled` returned by workers as a normal shutdown and combines other
errors with `errors.Join`.

A `Result` must have one consumer. Either receive from it directly or pass it
to `Wait`, but do not do both.

## Panics

Panics from `Worker.Execute` are recovered and logged with their value and stack
trace. One-shot modes report a `*gorker.PanicError` through their result.
Recurring modes log the failed activation and continue with the next one.

Use `errors.As` when a caller needs to distinguish a recovered panic:

```go
var panicError *gorker.PanicError
if errors.As(err, &panicError) {
	slog.Error("worker panicked", "error", panicError)
}
```

## Concurrency

One `Start` call executes its worker sequentially and never overlaps its own
activations. Starting the same `Worker` instance more than once may execute that
instance concurrently, so shared state remains the caller's responsibility.

Every `Start` call owns an independent lifecycle and returns an independent
`Result`.
