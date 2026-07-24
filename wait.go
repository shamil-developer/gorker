package gorker

import (
	"context"
	"errors"
)

// Wait waits for all results or until ctx expires.
// Context cancellation reported by workers is treated as a normal shutdown.
func Wait(ctx context.Context, results ...Result) error {
	if ctx == nil {
		return ErrNilContext
	}

	var workerErrors []error
	for _, result := range results {
		if result == nil {
			continue
		}

		select {
		case err := <-result:
			if err != nil && !errors.Is(err, context.Canceled) {
				workerErrors = append(workerErrors, err)
			}

		case <-ctx.Done():
			workerErrors = append(workerErrors, ctx.Err())
			return errors.Join(workerErrors...)
		}
	}

	return errors.Join(workerErrors...)
}
