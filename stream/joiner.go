package stream

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Joiner объединяет несколько потоков данных в один.
type Joiner[T any] struct {
	strms []*Stream[T]
}

// NewJoiner конструктор соединителя потоков данных.
func NewJoiner[T any](strm ...*Stream[T]) *Joiner[T] {
	return &Joiner[T]{strms: strm}
}

// Join добавляет поток/потоки к объединяемым потокам.
func (j *Joiner[T]) Join(strm ...*Stream[T]) *Joiner[T] {
	return &Joiner[T]{
		strms: append(j.strms, strm...),
	}
}

// Stream создает объединенный поток из child streams.
// Начинает транслировать данные из объединяемых потоков.
// После вызова этой функции добавление новых потоков ведет к неопределенному поведению.
func (j *Joiner[T]) Stream(ctx context.Context) *Stream[T] {
	closeChilds := func() {
		for _, strm := range j.strms {
			strm.Close()
		}
	}

	pipe := func(out *Stream[T], in *In[T]) error {
		for data := range out.Data() {
			if err := in.Sent(data); err != nil {
				return err
			}
		}
		return out.Err()
	}

	once := &sync.Once{}
	handler := func(ctx context.Context, in *In[T]) error {
		g, _ := errgroup.WithContext(ctx)
		for _, strm := range j.strms {
			out := strm
			g.Go(func() error {
				if err := pipe(out, in); err != nil {
					once.Do(func() { closeChilds() })
					return err
				}
				return nil
			})
		}
		return g.Wait()
	}

	return New(
		ctx,
		func(ctx context.Context, in *In[T]) error {
			err := handler(ctx, in)
			// ждет пока child streams закроются, только после этого закрывается сам.
			closeChilds()
			return err
		},
	)
}
