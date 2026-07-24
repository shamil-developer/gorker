// Package gorker provides asynchronous lifecycle management for background
// workers.
//
// A Worker contains only the business operation. Start combines it with a Mode
// that controls when it runs and a Config that controls timeouts, retries, and
// structured lifecycle logging.
package gorker
