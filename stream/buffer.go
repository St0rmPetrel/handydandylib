package stream

import "context"

// NewBuffer создает поток элемент которого slice элементов исходного потока,
// размером buffSize (может быть меньше в конце потока).
// То есть созданный поток накапливает элементы исходного потока и отдает их группами по buffSize.
func NewBuffer[T any](
	ctx context.Context,
	stream *Stream[T],
	buffSize int,
) *Stream[[]T] {
	return New(
		ctx,
		func(ctx context.Context, in *In[[]T]) error {
			defer stream.Close()
			buff := make([]T, 0, buffSize)

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case data, ok := <-stream.Data():
					if !ok {
						if len(buff) > 0 {
							if err := in.Sent(buff); err != nil {
								return err
							}
						}
						return stream.Err()
					}
					buff = append(buff, data)
					if len(buff) >= buffSize {
						if err := in.Sent(buff); err != nil {
							return err
						}
						buff = make([]T, 0, buffSize)
					}
				}
			}
		},
	)
}
