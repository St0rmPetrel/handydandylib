package batch

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// GrpcHandler обертка разбиения функции на батчи
type GrpcHandler[RespT, ReqT any] struct {
	// handle функция grpc ручки, вызов которой разбивается по батчам
	handle func(context.Context, ReqT, ...grpc.CallOption) (RespT, error)
	// iterator функция которая готовит реквест с батчем для handle
	// возвращает следующий реквсет, isLast, ok
	// внимание итератор должен быть потоко безопасен
	iterator func() (ReqT, bool, bool)
}

// NewGrpcHandler конструктор batch обертки grpc ручки
func NewGrpcHandler[RespT, ReqT any](
	handle func(context.Context, ReqT, ...grpc.CallOption) (RespT, error),
	iterator func() (ReqT, bool, bool),
) *GrpcHandler[RespT, ReqT] {
	return &GrpcHandler[RespT, ReqT]{
		handle:   handle,
		iterator: iterator,
	}
}

// Do метод выполнения батча (может быть параллельным или последовательным)
func (h *GrpcHandler[RespT, ReqT]) Do(
	ctx context.Context,
	goRoutinNum int,
) ([]RespT, error) {
	if goRoutinNum > 1 {
		return h.DoConcurrent(ctx, goRoutinNum)
	}
	return h.DoSerial(ctx)
}

// DoConcurrent метод выполнения одновременного батча
// на выходе получаем слайс результатов с непредсказуемой последовательностью относительно
// друг друга
func (h *GrpcHandler[RespT, ReqT]) DoConcurrent(
	ctx context.Context,
	goRoutinNum int,
) ([]RespT, error) {
	respSeries := make([]RespT, 0)
	g, gCtx := errgroup.WithContext(ctx)
	mutex := &sync.Mutex{}
	for i := 0; i < goRoutinNum; i++ {
		g.Go(func() error {
			for {
				req, isLast, ok := h.iterator()
				if !ok {
					break
				}

				response, err := h.handle(gCtx, req)
				if err != nil {
					return err
				}

				mutex.Lock()
				respSeries = append(respSeries, response)
				mutex.Unlock()

				if isLast {
					break
				}
			}
			return nil
		})
	}
	// ждем пока не отработают все батчи
	if err := g.Wait(); err != nil {
		if err != nil {
			return nil, err
		}
	}

	return respSeries, nil
}

// DoSerial метод выполнения последовательного батча
// на выходе получает упорядоченный по батчам слайс ответов на каждый
// из батчовых запросов
func (h *GrpcHandler[RespT, ReqT]) DoSerial(ctx context.Context) ([]RespT, error) {
	retRespSeries := make([]RespT, 0)
	for {
		req, isLast, ok := h.iterator()
		if !ok {
			break
		}

		response, err := h.handle(ctx, req)
		if err != nil {
			return nil, err
		}
		retRespSeries = append(retRespSeries, response)

		if isLast {
			break
		}
	}
	return retRespSeries, nil
}
