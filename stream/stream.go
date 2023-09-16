package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Stream поток данных, похож на канал, но с обработкой ошибок.
// Также в отличии от канала, поток не паникует и его можно закрывать несколько раз.
type Stream[T any] struct {
	dataCh  chan T
	closeCh chan struct{}
	err     error

	sync.WaitGroup
}

var errSentFail = fmt.Errorf("fail to sent data")

// Close принудительно закрывает канал.
// При этом после такого закрытия можно не вызывать strm.Err(),
// а если вызвать он вернет nil.
// Также после отработки данного метода канал strm.Data() будет гарантированно закрыт.
func (s *Stream[T]) Close() {
	s.close()
	// ждем когда закроется dataCh, что бы гарантировать что после Close из Data ничего не придет.
	s.Wait()
}

// Data возвращает канал данных, который может быть закрыт.
// Лучше всегда вынимать из него данные через range.
func (s *Stream[T]) Data() <-chan T {
	return s.dataCh
}

// Err проверяет закрылся ли поток по ошибке.
func (s *Stream[T]) Err() error {
	s.Wait()
	return s.err
}

func (s *Stream[T]) close() {
	defer func() {
		_ = recover()
	}()
	s.closeCh <- struct{}{}
}

// In вход потока, пользователь взаимодействует со входом данных только на этапе создания потока.
type In[T any] struct {
	ctx  context.Context
	strm *Stream[T]
}

// Sent отправляет данные в поток.
func (i *In[T]) Sent(data T) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = errSentFail
		}
	}()
	i.strm.dataCh <- data
	return
}

// New конструктор потока, сразу начинает транслировать данные.
func New[T any](
	ctx context.Context,
	handler func(ctx context.Context, in *In[T]) error,
) *Stream[T] {
	strm := Stream[T]{
		dataCh:  make(chan T),
		closeCh: make(chan struct{}),
	}

	// close stream
	strm.Add(1)
	go func() {
		defer strm.Done()

		select {
		case <-ctx.Done():
		case <-strm.closeCh:
		}
		close(strm.closeCh)
		close(strm.dataCh)
	}()

	in := &In[T]{
		ctx:  ctx,
		strm: &strm,
	}

	// handle error
	strm.Add(1)
	go func() {
		defer strm.Done()
		err := handler(ctx, in)
		strm.close()
		if err != nil {
			if errors.Is(err, errSentFail) {
				// context Canceled
				// or nil if stream is manual closed
				strm.err = ctx.Err()
			} else {
				// some unexpected handler error
				strm.err = err
			}
		}
	}()

	return &strm
}
