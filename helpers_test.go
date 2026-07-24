package gorker

import (
	"context"
	"sync"
)

type workerFunc func(context.Context) error

func (f workerFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

type modeFunc func(context.Context, func(context.Context) error) error

func (modeFunc) validate() error {
	return nil
}

func (f modeFunc) run(
	ctx context.Context,
	execute func(context.Context) error,
) error {
	return f(ctx, execute)
}

type logEntry struct {
	level   string
	message string
	args    []any
}

type recordingLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

func (l *recordingLogger) Debug(message string, args ...any) {
	l.record("debug", message, args)
}

func (l *recordingLogger) Info(message string, args ...any) {
	l.record("info", message, args)
}

func (l *recordingLogger) Warn(message string, args ...any) {
	l.record("warn", message, args)
}

func (l *recordingLogger) Error(message string, args ...any) {
	l.record("error", message, args)
}

func (e logEntry) value(key string) (any, bool) {
	for index := 0; index+1 < len(e.args); index += 2 {
		entryKey, ok := e.args[index].(string)
		if ok && entryKey == key {
			return e.args[index+1], true
		}
	}
	return nil, false
}

func (l *recordingLogger) record(level, message string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, logEntry{
		level:   level,
		message: message,
		args:    append([]any(nil), args...),
	})
}

func (l *recordingLogger) snapshot() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]logEntry(nil), l.entries...)
}
