# gorker

`gorker` — легковесная Go-библиотека для безопасного асинхронного запуска
фоновых воркеров.

Библиотека отделяет бизнес-логику от режима запуска и предоставляет timeout,
retry, panic recovery, структурированные lifecycle-логи и graceful shutdown.

## Установка

```sh
go get github.com/shamil-developer/gorker
```

## Быстрый старт

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/shamil-developer/gorker"
)

type cleanupWorker struct{}

func (cleanupWorker) Execute(ctx context.Context) error {
	// Бизнес-логика очистки.
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	result := gorker.Start(
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

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := gorker.Wait(shutdownCtx, result); err != nil {
		slog.Error("workers stopped with an error", "error", err)
	}
}
```

`Start` валидирует параметры, запускает отдельную горутину и сразу возвращает
`Result` (`<-chan error`). При успешном завершении канал закрывается без
значения. При окончательной ошибке он возвращает одну ошибку и закрывается.

Один `Result` должен иметь одного читателя: либо читайте его самостоятельно,
либо передавайте в `Wait`.

## Режимы

| Режим | Поведение |
| --- | --- |
| `Once` | Один немедленный запуск |
| `Delayed` | Один запуск после задержки |
| `Scheduled` | Один запуск в заданный момент |
| `Periodic` | Запуск по фиксированному интервалу |
| `FixedDelay` | Задержка отсчитывается после завершения предыдущего запуска |
| `Cron` | Последовательный запуск по cron-выражению |

Режимы не запускают несколько выполнений одного воркера одновременно.
Пропущенные срабатывания не накапливаются.

`Cron` использует локальную временную зону процесса. Другую зону можно указать
непосредственно в выражении:

```go
gorker.Cron{
	Expression: "CRON_TZ=Europe/Moscow 0 3 * * *",
}
```

## Retry

Нулевое значение `Retry` означает одну попытку без повторов. При настройке
задержка удваивается после каждой ошибки и не превышает `MaxDelay`:

```go
gorker.Retry{
	MaxAttempts:  5,
	InitialDelay: time.Second,
	MaxDelay:     8 * time.Second,
}
```

Задержки в этом примере: `1s`, `2s`, `4s`, `8s`. Чтобы получить постоянную
задержку, задайте одинаковые `InitialDelay` и `MaxDelay`.

`MaxAttempts` включает первую попытку и должен быть не меньше двух. Timeout
применяется отдельно к каждой попытке. Отмена родительского context немедленно
прекращает retry.

## Logger

Logger является обязательной DI-зависимостью:

```go
type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}
```

Интерфейсу соответствует `*slog.Logger`. Переданная реализация должна быть
безопасна для конкурентного использования.

## Завершение

Для нескольких воркеров отмените общий context и дождитесь всех результатов:

```go
if err := gorker.Wait(
	shutdownCtx,
	firstResult,
	secondResult,
	thirdResult,
); err != nil {
	// Обработка окончательных ошибок воркеров.
}
```

Timeout и отмена context кооперативны: реализация `Worker` должна учитывать
`ctx.Done()` и завершать долгие операции.

Короткие компилируемые примеры для `Start`, всех встроенных режимов и `Wait`
находятся в [`example_test.go`](example_test.go). После публикации они также
будут отображаться на pkg.go.dev.
