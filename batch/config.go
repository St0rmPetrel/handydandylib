package batch

import (
	"fmt"
)

// HandlerConfig конфигурация батчевой обертки ручки
type HandlerConfig struct {
	// BatchSize размер пакета
	BatchSize int
	// GoRoutineNum количество go рутин в параллельной обработке пакетов,
	GoRoutineNum int
}

// NewHandlerConfig конструктор конфигурации батчевой оберти ручки
func NewHandlerConfig(batchSize, goRoutineNum int) (*HandlerConfig, error) {
	ret := &HandlerConfig{
		BatchSize:    batchSize,
		GoRoutineNum: goRoutineNum,
	}
	if err := ret.validate(); err != nil {
		return nil, err
	}
	return ret, nil
}

// validate проверяет настройки на правильность
func (c *HandlerConfig) validate() error {
	if c.GoRoutineNum <= 0 {
		return fmt.Errorf("batch: handler_config: goroutine number = %d must be greater than 0", c.GoRoutineNum)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("batch: handler_config: batch size = %d must be greater than 0", c.BatchSize)
	}
	return nil
}
