package gorker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/shamil-developer/gorker"
)

type cleanupWorker struct{}

func (cleanupWorker) Execute(context.Context) error {
	fmt.Println("cleanup completed")
	return nil
}

type heartbeatWorker struct {
	cancel context.CancelFunc
}

func (w heartbeatWorker) Execute(context.Context) error {
	fmt.Println("heartbeat")
	w.cancel()
	return nil
}

type silentWorker struct{}

func (silentWorker) Execute(context.Context) error {
	return nil
}

func exampleLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func ExampleStart() {
	result, err := gorker.Start(
		context.Background(),
		"cleanup",
		cleanupWorker{},
		gorker.Once{},
		gorker.Config{
			Timeout: 5 * time.Second,
			Retry: gorker.Retry{
				MaxAttempts:  3,
				InitialDelay: time.Second,
				MaxDelay:     4 * time.Second,
			},
			Logger: exampleLogger(),
		},
	)
	if err != nil {
		fmt.Println("start:", err)
		return
	}

	if err := <-result; err != nil {
		fmt.Println("run:", err)
	}

	// Output:
	// cleanup completed
}

func ExamplePeriodic() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := gorker.Start(
		ctx,
		"heartbeat",
		heartbeatWorker{cancel: cancel},
		gorker.Periodic{
			Interval:  time.Minute,
			Immediate: true,
		},
		gorker.Config{Logger: exampleLogger()},
	)
	if err != nil {
		fmt.Println("start:", err)
		return
	}

	if err := gorker.Wait(context.Background(), result); err != nil {
		fmt.Println("wait:", err)
	}

	// Output:
	// heartbeat
}

func ExampleWait() {
	first, err := gorker.Start(
		context.Background(),
		"first",
		silentWorker{},
		gorker.Once{},
		gorker.Config{Logger: exampleLogger()},
	)
	if err != nil {
		fmt.Println("start first:", err)
		return
	}

	second, err := gorker.Start(
		context.Background(),
		"second",
		silentWorker{},
		gorker.Once{},
		gorker.Config{Logger: exampleLogger()},
	)
	if err != nil {
		fmt.Println("start second:", err)
		return
	}

	if err := gorker.Wait(context.Background(), first, second); err != nil {
		fmt.Println("wait:", err)
		return
	}
	fmt.Println("all workers completed")

	// Output:
	// all workers completed
}
