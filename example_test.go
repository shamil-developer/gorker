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
		fmt.Println("error:", err)
	}

}

func ExamplePeriodic() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := gorker.Start(
		ctx,
		"heartbeat",
		silentWorker{},
		gorker.Periodic{
			Interval:  time.Minute,
			Immediate: true,
		},
		gorker.Config{
			Logger: exampleLogger(),
		},
	)
	if err != nil {
		fmt.Println("start:", err)
		return
	}

	cancel()
	_ = gorker.Wait(context.Background(), result)
}

func ExampleFixedDelay() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := gorker.Start(
		ctx,
		"poller",
		silentWorker{},
		gorker.FixedDelay{
			Delay:     time.Minute,
			Immediate: true,
		},
		gorker.Config{
			Logger: exampleLogger(),
		},
	)
	if err != nil {
		fmt.Println("start:", err)
		return
	}

	cancel()
	_ = gorker.Wait(context.Background(), result)
}

func ExampleDelayed() {
	result, err := gorker.Start(
		context.Background(),
		"delayed_cleanup",
		silentWorker{},
		gorker.Delayed{
			Delay: time.Minute,
		},
		gorker.Config{
			Logger: exampleLogger(),
		},
	)
	if err != nil {
		fmt.Println("start:", err)
		return
	}

	_ = result
}

func ExampleScheduled() {
	result, err := gorker.Start(
		context.Background(),
		"scheduled_report",
		silentWorker{},
		gorker.Scheduled{
			At: time.Now().Add(time.Hour),
		},
		gorker.Config{
			Logger: exampleLogger(),
		},
	)
	if err != nil {
		fmt.Println("start:", err)
		return
	}

	_ = result
}

func ExampleCron() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := gorker.Start(
		ctx,
		"nightly_cleanup",
		silentWorker{},
		gorker.Cron{
			Expression: "CRON_TZ=Europe/Moscow 0 3 * * *",
		},
		gorker.Config{
			Logger: exampleLogger(),
		},
	)
	if err != nil {
		fmt.Println("start:", err)
		return
	}

	cancel()
	_ = gorker.Wait(context.Background(), result)
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
		fmt.Println("start:", err)
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
		fmt.Println("start:", err)
		return
	}

	if err := gorker.Wait(context.Background(), first, second); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("all workers completed")

}
