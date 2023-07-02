package breaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Breaker is a simple implementation of a Circuit breaker design pattern
//
// It is used to detect failures and encapsulates the logic of
// preventing a failure
func Breaker[RespT, ReqT any](
	handler func(context.Context, ReqT) (RespT, error),
	opts ...Option,
) func(context.Context, ReqT) (RespT, error) {
	config := newDefaultOptions()
	for _, opt := range opts {
		opt(config)
	}
	var (
		lastAttempt         = time.Now()
		consecutiveFailures = 0
		m                   sync.RWMutex
	)
	return func(ctx context.Context, req ReqT) (RespT, error) {
		m.RLock()

		failures := consecutiveFailures - int(config.failureThreshold)
		if failures > 0 {
			shouldRetryAt := lastAttempt.Add(config.breakDelay)
			if !time.Now().After(shouldRetryAt) {
				m.RUnlock()
				config.breakCallback(failures, lastAttempt, shouldRetryAt)
				var nilResp RespT
				return nilResp, config.unreachableError
			}
		}

		m.RUnlock()

		resp, err := handler(ctx, req)

		m.Lock()
		defer m.Unlock()

		lastAttempt = time.Now()

		if err != nil {
			consecutiveFailures++
			return resp, err
		}

		consecutiveFailures = 0

		return resp, err
	}
}

// Callback функция которая вызывается каждый раз после размыкания цепи
// обычно используется для логов
type Callback func(failures int, lastAttempt, shouldRetryAt time.Time)

// Option функция для изменения настроек поведения Breaker-а
type Option func(o *options)

type options struct {
	failureThreshold uint
	unreachableError error
	breakDelay       time.Duration
	breakCallback    Callback
}

// Настройки Breaker-а по умолчанию
var (
	defaultFailureThreshold uint  = 10
	defaultUnreachableError error = errors.New("service is unreachble")
	defaultBreakDelay             = 2 * time.Second
	defaultBreakCallback          = func(_ int, _, _ time.Time) {}
)

// newDefaultOptions конструктор настроек по умолчанию
func newDefaultOptions() *options {
	return &options{
		failureThreshold: defaultFailureThreshold,
		unreachableError: defaultUnreachableError,
		breakDelay:       defaultBreakDelay,
		breakCallback:    defaultBreakCallback,
	}
}

// WithFailureThreshold настройка порога количества ошибок идущих подряд,
// после превышения которого цепь на время размыкается
func WithFailureThreshold(failureThreshold uint) Option {
	return func(o *options) {
		o.failureThreshold = failureThreshold
	}
}

// WithUnreachableError настройка ошибки возвращаемой во время того как цепь
// разомкнута (обычно это ошибка что сервис не доступен)
func WithUnreachableError(errUnreachable error) Option {
	return func(o *options) {
		o.unreachableError = errUnreachable
	}
}

// WithBreakDelay настройка времени на сколько цепь размыкается в случае сбоя
func WithBreakDelay(delay time.Duration) Option {
	return func(o *options) {
		o.breakDelay = delay
	}
}

// WithBreakCallback настройка функции callback вызываемой после попытки запроса
// при разомкнутой цепи
func WithBreakCallback(callback Callback) Option {
	return func(o *options) {
		o.breakCallback = callback
	}
}
