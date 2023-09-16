package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinerPositive(t *testing.T) {
	tests := map[string]struct {
		streamA *Stream[int]
		streamB *Stream[int]

		expected []int
	}{
		"empty": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return nil
				},
			),
			streamB: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return nil
				},
			),
			expected: []int{},
		},
		"1": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return in.Sent(1)
				},
			),
			streamB: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return in.Sent(1)
				},
			),
			expected: []int{1, 1},
		},
		"1_10_even_odd": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					for i := 1; i <= 10; i++ {
						if i%2 == 0 {
							if err := in.Sent(i); err != nil {
								return err
							}
						}
					}
					return nil
				},
			),
			streamB: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					for i := 1; i <= 10; i++ {
						if i%2 != 0 {
							if err := in.Sent(i); err != nil {
								return err
							}
						}
					}
					return nil
				},
			),
			expected: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			joiner := NewJoiner(test.streamA, test.streamB)

			strm := joiner.Stream(context.Background())
			resp := make([]int, 0)
			for data := range strm.Data() {
				resp = append(resp, data)
			}
			require.NoError(t, strm.Err())

			require.ElementsMatch(t, resp, test.expected)
		})
	}
}

func TestJoinerNegative(t *testing.T) {
	streamErr := fmt.Errorf("some error")
	tests := map[string]struct {
		streamA *Stream[int]
		streamB *Stream[int]

		expectedErr error
	}{
		"both_err": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return streamErr
				},
			),
			streamB: New(
				context.Background(),
				func(ctx context.Context, in *In[int]) error {
					return streamErr
				},
			),
			expectedErr: streamErr,
		},
		"one_err": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return streamErr
				},
			),
			streamB: New(
				context.Background(),
				func(ctx context.Context, in *In[int]) error {
					for {
						if err := in.Sent(1); err != nil {
							return err
						}
					}
				},
			),
			expectedErr: streamErr,
		},
		"deferred_one_err": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					for i := 1; i <= 10; i++ {
						if err := in.Sent(i); err != nil {
							return err
						}
					}
					return streamErr
				},
			),
			streamB: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					for {
						if err := in.Sent(1); err != nil {
							return err
						}
					}
				},
			),
			expectedErr: streamErr,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			joiner := NewJoiner(test.streamA, test.streamB)

			strm := joiner.Stream(context.Background())
			for range strm.Data() {
			}
			require.Equal(t, test.expectedErr, strm.Err())
		})
	}
}

func TestJoinerClose(t *testing.T) {
	tests := map[string]struct {
		streamA *Stream[int]
		streamB *Stream[int]
		lastEl  int
	}{
		"sent_1": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return in.Sent(1)
				},
			),
			streamB: New(
				context.Background(),
				func(ctx context.Context, in *In[int]) error {
					return in.Sent(1)
				},
			),
			lastEl: 1,
		},
		"infinity_sent_10_even_odd": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					i := 1
					for {
						if i%2 == 0 {
							if err := in.Sent(i); err != nil {
								return err
							}
						}
						i++
					}
				},
			),
			streamB: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					i := 1
					for {
						if i%2 != 0 {
							if err := in.Sent(i); err != nil {
								return err
							}
						}
						i++
					}
				},
			),
			lastEl: 10,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			joiner := NewJoiner(test.streamA, test.streamB)
			strm := joiner.Stream(context.Background())

			prev := -1
			for data := range strm.Data() {
				if data >= test.lastEl {
					// must be not data after strm.Close()
					// but can be received for example 7 9 11 (skip 10) in infinity_sent_10_even_odd test.
					require.True(t, data >= test.lastEl && prev <= data)
					strm.Close()
				}
				prev = data
			}
			require.NoError(t, strm.Err())
		})
	}
}

func TestJoinerCancelCtx(t *testing.T) {
	tests := map[string]struct {
		streamA *Stream[int]
		streamB *Stream[int]
		lastEl  int
	}{
		"sent_1": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return nil
				},
			),
			streamB: New(
				context.Background(),
				func(ctx context.Context, in *In[int]) error {
					for {
						if err := in.Sent(1); err != nil {
							return err
						}
					}
				},
			),
			lastEl: 1,
		},
		"infinity_sent_10_even_odd": {
			streamA: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					i := 1
					for {
						if i%2 == 0 {
							if err := in.Sent(i); err != nil {
								return err
							}
						}
						i++
					}
				},
			),
			streamB: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					i := 1
					for {
						if i%2 != 0 {
							if err := in.Sent(i); err != nil {
								return err
							}
						}
						i++
					}
				},
			),
			lastEl: 10,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			joiner := NewJoiner(test.streamA, test.streamB)
			strm := joiner.Stream(ctx)

			for data := range strm.Data() {
				if data >= test.lastEl {
					cancel()
				}
			}
			require.ErrorIs(t, context.Canceled, strm.Err())
		})
	}
}
