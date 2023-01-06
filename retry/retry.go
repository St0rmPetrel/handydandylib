package retry

import "time"

// Do сделать в несколько попыток
func Do(retryableFunc RetryableFunc, opts ...Option) error {
	retryOptions := newDefaultOptions()
	for _, opt := range opts {
		opt(retryOptions)
	}

	var attempt uint
	for {
		attempt++
		err := retryableFunc()
		if err != nil {
			retryOptions.retryFailCallback(attempt, err)
			if attempt > retryOptions.retryCount {
				return err
			}
			time.Sleep(retryOptions.retryDelay)
			continue
		}
		return nil
	}
}

// WithFailCallback настраивает функцию которая исполняется после очередной
// неудачной попытки
func WithFailCallback(callback func(uint, error)) Option {
	return func(opts *options) {
		opts.retryFailCallback = callback
	}
}

// WithRetryCount настраивает количество попыток после которых Do перестает пытаться и завершается с ошибкой
func WithRetryCount(count uint) Option {
	return func(opts *options) {
		opts.retryCount = count
	}
}

// WithRetryDelay настраивает длительность паузы перед новой попыткой
func WithRetryDelay(delay time.Duration) Option {
	return func(opts *options) {
		opts.retryDelay = delay
	}
}

// RetryableFunc функция вызов которой повторяют в случае неудачи
type RetryableFunc func() error

// Option функция для настройки повеления Do
type Option func(opts *options)

const (
	// DefaultRetryCount количество попыток повтора по умолчанию
	DefaultRetryCount uint = 3
	// DefaultRetryDelay длительность задержки перед новой попыткой по умолчанию
	DefaultRetryDelay time.Duration = time.Second
)

// DefaultRetryCallback функция вызываемая после безуспешной попытки по умолчанию
func DefaultRetryCallback(_ uint, _ error) {}

// options настройки поведения Do
type options struct {
	retryCount        uint
	retryDelay        time.Duration
	retryFailCallback func(uint, error)
}

// newDefaultOptions конструктор настроек по умолчанию
func newDefaultOptions() *options {
	return &options{
		retryCount:        DefaultRetryCount,
		retryDelay:        DefaultRetryDelay,
		retryFailCallback: DefaultRetryCallback,
	}
}
