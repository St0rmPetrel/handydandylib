package stream

import (
	"context"
	"sync"
)

// Stream поток данных, похож на канал, но с обработкой ошибок и без нужды
// корректного закрытия, так как закрывается сам после осушения.
// Также в отличии от канала, поток не паникует.
type Stream[T any] struct {
	dataCh chan T
	errCh  chan error
	cancel func()
	close  *sync.WaitGroup
}

// Close закрывает канал, после закрытия по каналу гарантированно не придут данные.
// Можно закрывать несколько раз.
func (s *Stream[T]) Close() {
	s.cancel()
	s.close.Wait()
}

// Data возвращает канал данных, который может быть закрыт.
// Лучше всегда вынимать из него данные через range.
func (s *Stream[T]) Data() <-chan T {
	return s.dataCh
}

// Err проверяет закрылся ли поток по ошибке.
// Нужно проверять корректно ли закрылся поток ВСЕГДА, иначе будет утечка.
// Даже когда мы закрыли канал через Close, нужно проверить что Err() == context.Canceled.
// В случае Err() == nil, поток был осушен без ошибок.
func (s *Stream[T]) Err() error {
	err, ok := <-s.errCh
	if !ok {
		return nil
	}
	return err
}

// StreamIn вход потока, пользователь взаимодействует со входом данных только на этапе создания потока.
type StreamIn[T any] struct {
	dataCh chan<- T
	ctx    context.Context
}

// Sent отправляет данные в поток.
func (si *StreamIn[T]) Sent(data T) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = si.ctx.Err()
		}
	}()
	si.dataCh <- data
	return
}

// New конструктор потока, сразу начинает транслировать данные.
func New[T any](
	ctx context.Context,
	handler func(ctx context.Context, in *StreamIn[T]) error,
) *Stream[T] {
	cctx, cancel := context.WithCancel(ctx)
	retCh := make(chan T)
	errCh := make(chan error)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-cctx.Done()
		close(retCh)
	}()

	in := &StreamIn[T]{
		dataCh: retCh,
		ctx:    cctx,
	}

	streamF := func(ctx context.Context, in *StreamIn[T]) error {
		defer cancel()
		return handler(ctx, in)
	}
	go func() {
		defer close(errCh)

		if err := streamF(cctx, in); err != nil {
			errCh <- err
			return
		}
	}()

	return &Stream[T]{
		dataCh: retCh,
		errCh:  errCh,
		cancel: cancel,
		close:  wg,
	}
}
