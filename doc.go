// Package gorker runs background workers with configurable scheduling,
// per-attempt timeouts, retries, structured logging, panic recovery, and
// graceful shutdown.
//
// A Worker contains the business operation. Start combines it with a Mode that
// controls when it runs and a Config that controls how each execution is
// handled.
//
// Start validates its arguments synchronously and then runs the worker
// asynchronously. Applications stop workers by canceling their contexts and
// use Wait to wait for completion.
package gorker
