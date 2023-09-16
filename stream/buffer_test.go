package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferPositive(t *testing.T) {
	tests := map[string]struct {
		stream   *Stream[int]
		buffSize int

		expected [][]int
	}{
		"empty": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return nil
				},
			),
			buffSize: 2,
			expected: [][]int{},
		},
		"one": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					return in.Sent(1)
				},
			),
			buffSize: 2,
			expected: [][]int{{1}},
		},
		"1_2_10": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					for i := 1; i <= 2; i++ {
						if err := in.Sent(i); err != nil {
							return err
						}
					}
					return nil
				},
			),
			buffSize: 10,
			expected: [][]int{{1, 2}},
		},
		"1_10_2": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					for i := 1; i <= 10; i++ {
						if err := in.Sent(i); err != nil {
							return err
						}
					}
					return nil
				},
			),
			buffSize: 2,
			expected: [][]int{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}},
		},
		"1_10_3": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					for i := 1; i <= 10; i++ {
						if err := in.Sent(i); err != nil {
							return err
						}
					}
					return nil
				},
			),
			buffSize: 3,
			expected: [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			buffSt := NewBuffer(context.Background(), test.stream, test.buffSize)

			resp := make([][]int, 0)
			for data := range buffSt.Data() {
				resp = append(resp, data)
			}
			require.NoError(t, buffSt.Err())
			require.ElementsMatch(t, resp, test.expected)
		})
	}
}

func TestBufferNegative(t *testing.T) {
	streamErr := fmt.Errorf("some err")
	tests := map[string]struct {
		stream      *Stream[int]
		buffSize    int
		expectedErr error
	}{
		"immediately_err": {
			stream: New(
				context.Background(),
				func(_ context.Context, _ *In[int]) error {
					return streamErr
				},
			),
			expectedErr: streamErr,
			buffSize:    2,
		},
		"deffer_err": {
			stream: New(
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
			buffSize:    2,
			expectedErr: streamErr,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			buffSt := NewBuffer(context.Background(), test.stream, test.buffSize)

			for range buffSt.Data() {
			}
			require.Equal(t, test.expectedErr, buffSt.Err())
		})
	}
}

func TestBufferClose(t *testing.T) {
	tests := map[string]struct {
		stream   *Stream[int]
		buffSize int
		lastEl   []int
		expected [][]int
	}{
		"sent_1": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					if err := in.Sent(1); err != nil {
						return err
					}
					return in.Sent(1)
				},
			),
			expected: [][]int{{1}},
			lastEl:   []int{1},
			buffSize: 1,
		},
		"infinity_sent_10": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *In[int]) error {
					i := 1
					for {
						if err := in.Sent(i); err != nil {
							return err
						}
						i++
					}
				},
			),
			buffSize: 2,
			expected: [][]int{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}},
			lastEl:   []int{9, 10},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			buffSt := NewBuffer(context.Background(), test.stream, test.buffSize)

			resp := make([][]int, 0)
			for data := range buffSt.Data() {
				resp = append(resp, data)
				if sliceEq(data, test.lastEl) {
					buffSt.Close()
				}
			}
			require.NoError(t, buffSt.Err())
			require.ElementsMatch(t, resp, test.expected)
		})
	}
}

func TestBufferCancelCtx(t *testing.T) {
	tests := map[string]struct {
		streamH  func(context.Context, *In[int]) error
		buffSize int
		lastEl   []int
		expected [][]int
	}{
		"sent_1": {
			streamH: func(_ context.Context, in *In[int]) error {
				for {
					if err := in.Sent(1); err != nil {
						return err
					}
				}
			},
			lastEl:   []int{1},
			buffSize: 1,
		},
		"infinity_sent_10": {
			streamH: func(_ context.Context, in *In[int]) error {
				i := 1
				for {
					if err := in.Sent(i); err != nil {
						return err
					}
					i++
				}
			},
			buffSize: 2,
			lastEl:   []int{9, 10},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			buffSt := NewBuffer(ctx, New(ctx, test.streamH), test.buffSize)

			for data := range buffSt.Data() {
				if sliceEq(data, test.lastEl) {
					cancel()
				}
			}
			require.ErrorIs(t, context.Canceled, buffSt.Err())
		})
	}
}

func sliceEq(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
