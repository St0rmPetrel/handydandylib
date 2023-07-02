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
	// next функция которая возвращает следующий request
	// если следующего нет, то возвращает (nil, false)
	// внимание итератор должен быть потоко-безопасен
	next func() (ReqT, bool)
}

// NewGrpcHandler конструктор batch обертки grpc ручки
func NewGrpcHandler[RespT, ReqT any](
	handle func(context.Context, ReqT, ...grpc.CallOption) (RespT, error),
	next func() (ReqT, bool),
) *GrpcHandler[RespT, ReqT] {
	return &GrpcHandler[RespT, ReqT]{
		handle: handle,
		next:   next,
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
				req, ok := h.next()
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
		req, ok := h.next()
		if !ok {
			break
		}

		response, err := h.handle(ctx, req)
		if err != nil {
			return nil, err
		}
		retRespSeries = append(retRespSeries, response)
	}
	return retRespSeries, nil
}
