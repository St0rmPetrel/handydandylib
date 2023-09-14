package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamPositive(t *testing.T) {
	tests := map[string]struct {
		stream *Stream[int]

		expected []int
	}{
		"empty": {
			stream: New(
				context.Background(),
				func(_ context.Context, _ *StreamIn[int]) error {
					return nil
				},
			),
			expected: []int{},
		},
		"1": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *StreamIn[int]) error {
					return in.Sent(1)
				},
			),
			expected: []int{1},
		},
		"1_10": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *StreamIn[int]) error {
					for i := 1; i <= 10; i++ {
						if err := in.Sent(i); err != nil {
							return err
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

			resp := make([]int, 0)
			for data := range test.stream.Data() {
				resp = append(resp, data)
			}
			require.NoError(t, test.stream.Err())
			require.ElementsMatch(t, resp, test.expected)
		})
	}
}

func TestStreamNegative(t *testing.T) {
	streamErr := fmt.Errorf("some err")
	tests := map[string]struct {
		stream      *Stream[int]
		expectedErr error
	}{
		"immediately_err": {
			stream: New(
				context.Background(),
				func(_ context.Context, _ *StreamIn[int]) error {
					return streamErr
				},
			),
			expectedErr: streamErr,
		},
		"deferred_err": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *StreamIn[int]) error {
					for i := 1; i <= 10; i++ {
						if err := in.Sent(i); err != nil {
							return err
						}
					}
					return streamErr
				},
			),
			expectedErr: streamErr,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			for range test.stream.Data() {
			}
			require.Equal(t, test.stream.Err(), test.expectedErr)
		})
	}
}

func TestStreamClose(t *testing.T) {
	tests := map[string]struct {
		stream   *Stream[int]
		lastEl   int
		expected []int
	}{
		"sent_1": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *StreamIn[int]) error {
					if err := in.Sent(1); err != nil {
						return err
					}
					return in.Sent(1)
				},
			),
			expected: []int{1},
			lastEl:   1,
		},
		"infinity_sent_10": {
			stream: New(
				context.Background(),
				func(_ context.Context, in *StreamIn[int]) error {
					i := 1
					for {
						if err := in.Sent(i); err != nil {
							return err
						}
						i++
					}
				},
			),
			expected: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			lastEl:   10,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			resp := make([]int, 0)
			for data := range test.stream.Data() {
				resp = append(resp, data)
				if data >= test.lastEl {
					require.Equal(t, data, test.lastEl)
					test.stream.Close()
				}
			}
			require.Equal(t, context.Canceled, test.stream.Err())
			require.ElementsMatch(t, resp, test.expected)
		})
	}
}
