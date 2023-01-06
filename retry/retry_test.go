package retry

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDoSimpleNegative(t *testing.T) {
	var accum int

	tests := []struct {
		name               string
		inputFailCallBack  func(uint, error)
		inputRetryCount    uint
		inputRetryDelay    time.Duration
		inputRetryableFunc func() error
		wantMaxExecTime    time.Duration
		wantAccum          int
	}{
		{
			name:               "no_retry",
			inputRetryCount:    0,
			inputRetryableFunc: func() error { return fmt.Errorf("") },
			inputFailCallBack:  func(attemt uint, _ error) { accum = int(attemt) },
			wantAccum:          1,
			inputRetryDelay:    5 * time.Second,
			wantMaxExecTime:    1 * time.Second,
		},
		{
			name:               "one_retry",
			inputRetryCount:    1,
			inputRetryableFunc: func() error { return fmt.Errorf("") },
			inputFailCallBack:  func(attemt uint, _ error) { accum = int(attemt) },
			wantAccum:          2,
			inputRetryDelay:    100 * time.Millisecond,
			wantMaxExecTime:    200 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accum = 0
			start := time.Now()
			err := Do(
				test.inputRetryableFunc,
				WithRetryCount(test.inputRetryCount),
				WithRetryDelay(test.inputRetryDelay),
				WithFailCallback(test.inputFailCallBack),
			)
			execDur := time.Since(start)
			require.Error(t, err)
			require.Equal(t, test.wantAccum, accum)
			require.Less(t, execDur, test.wantMaxExecTime)
		})
	}
}

func TestDoSimplePositive(t *testing.T) {
	var accum int

	tests := []struct {
		name               string
		inputFailCallBack  func(uint, error)
		inputRetryCount    uint
		inputRetryDelay    time.Duration
		inputRetryableFunc func() error
		wantMaxExecTime    time.Duration
		wantAccum          int
	}{
		{
			name:               "no_retry",
			inputRetryCount:    0,
			inputRetryableFunc: func() error { accum = 5; return nil },
			inputFailCallBack:  func(_ uint, _ error) { accum = -100 },
			wantAccum:          5,
			inputRetryDelay:    5 * time.Second,
			wantMaxExecTime:    1 * time.Second,
		},
		{
			name:            "one_retry",
			inputRetryCount: 1,
			inputRetryableFunc: func() error {
				if accum < 5 {
					return fmt.Errorf("")
				}
				accum = 1
				return nil
			},
			inputFailCallBack: func(attemt uint, _ error) { accum = 10 * int(attemt) },
			wantAccum:         1,
			inputRetryDelay:   100 * time.Millisecond,
			wantMaxExecTime:   200 * time.Millisecond,
		},
		{
			name:            "many_retry_fibonacci",
			inputRetryCount: 100,
			inputRetryableFunc: func() func() error {
				a, b := 1, 1
				return func() error {
					tempB := b
					b = a + b
					a = tempB
					if accum >= 0 {
						return fmt.Errorf("")
					}
					accum = a
					return nil
				}
			}(),
			inputFailCallBack: func(attemt uint, _ error) {
				if attemt == 7 {
					accum = -1
				}
			},
			// 34 is 7d (from 2) fibonacci number
			wantAccum:       34,
			inputRetryDelay: 100 * time.Millisecond,
			wantMaxExecTime: time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accum = 0
			start := time.Now()
			err := Do(
				test.inputRetryableFunc,
				WithRetryCount(test.inputRetryCount),
				WithRetryDelay(test.inputRetryDelay),
				WithFailCallback(test.inputFailCallBack),
			)
			execDur := time.Since(start)
			require.NoError(t, err)
			require.Equal(t, test.wantAccum, accum)
			require.Less(t, execDur, test.wantMaxExecTime)
		})
	}
}

func TestDoMutateDelayPositive(t *testing.T) {
	tests := []struct {
		name                   string
		inputRetryCount        uint
		inputRetryDelay        time.Duration
		inputRetryableFunc     func() error
		inputDelayMutationFunc func(time.Duration) time.Duration
		wantMaxExecTime        time.Duration
		wantMinExecTime        time.Duration
	}{
		{
			name:            "default_duration_mutation_func",
			inputRetryCount: 5,
			inputRetryDelay: time.Millisecond,
			inputRetryableFunc: func() func() error {
				counter := 0
				return func() error {
					defer func() { counter++ }()
					if counter < 3 {
						return fmt.Errorf("")
					}
					return nil
				}
			}(),
			inputDelayMutationFunc: DefaultMutateRetryDelay,
			wantMaxExecTime:        5 * time.Millisecond,
			wantMinExecTime:        3 * time.Millisecond,
		},
		{
			name:            "factor_duration_mutation_func",
			inputRetryCount: 5,
			inputRetryDelay: time.Millisecond,
			inputRetryableFunc: func() func() error {
				counter := 0
				return func() error {
					defer func() { counter++ }()
					if counter < 3 {
						return fmt.Errorf("")
					}
					return nil
				}
			}(),
			inputDelayMutationFunc: FactorMutateRetryDelay,
			wantMaxExecTime:        9 * time.Millisecond,
			wantMinExecTime:        7 * time.Millisecond,
		},
		{
			name:            "exp_duration_mutation_func",
			inputRetryCount: 5,
			inputRetryDelay: time.Millisecond,
			inputRetryableFunc: func() func() error {
				counter := 0
				return func() error {
					defer func() { counter++ }()
					if counter < 4 {
						return fmt.Errorf("")
					}
					return nil
				}
			}(),
			inputDelayMutationFunc: ExpMutateRetryDelay,
			wantMaxExecTime:        18 * time.Millisecond,
			wantMinExecTime:        15 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := time.Now()
			err := Do(
				test.inputRetryableFunc,
				WithRetryCount(test.inputRetryCount),
				WithRetryDelay(test.inputRetryDelay),
				WithRetryDelayMutation(test.inputDelayMutationFunc),
			)
			execDur := time.Since(start)

			require.NoError(t, err)
			require.Less(t, execDur, test.wantMaxExecTime)
			require.Greater(t, execDur, test.wantMinExecTime)
		})
	}
}

func TestDoMutateDelayNegative(t *testing.T) {
	tests := []struct {
		name                   string
		inputRetryCount        uint
		inputRetryDelay        time.Duration
		inputRetryableFunc     func() error
		inputDelayMutationFunc func(time.Duration) time.Duration
		wantMaxExecTime        time.Duration
		wantMinExecTime        time.Duration
	}{
		{
			name:            "default_duration_mutation_func",
			inputRetryCount: 3,
			inputRetryDelay: time.Millisecond,
			inputRetryableFunc: func() func() error {
				counter := 0
				return func() error {
					defer func() { counter++ }()
					if counter < 5 {
						return fmt.Errorf("")
					}
					return nil
				}
			}(),
			inputDelayMutationFunc: DefaultMutateRetryDelay,
			wantMaxExecTime:        4 * time.Millisecond,
			wantMinExecTime:        3 * time.Millisecond,
		},
		{
			name:            "factor_duration_mutation_func",
			inputRetryCount: 3,
			inputRetryDelay: time.Millisecond,
			inputRetryableFunc: func() func() error {
				counter := 0
				return func() error {
					defer func() { counter++ }()
					if counter < 5 {
						return fmt.Errorf("")
					}
					return nil
				}
			}(),
			inputDelayMutationFunc: FactorMutateRetryDelay,
			wantMaxExecTime:        9 * time.Millisecond,
			wantMinExecTime:        7 * time.Millisecond,
		},
		{
			name:            "exp_duration_mutation_func",
			inputRetryCount: 4,
			inputRetryDelay: time.Millisecond,
			inputRetryableFunc: func() func() error {
				counter := 0
				return func() error {
					defer func() { counter++ }()
					if counter < 5 {
						return fmt.Errorf("")
					}
					return nil
				}
			}(),
			inputDelayMutationFunc: ExpMutateRetryDelay,
			wantMaxExecTime:        18 * time.Millisecond,
			wantMinExecTime:        15 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := time.Now()
			err := Do(
				test.inputRetryableFunc,
				WithRetryCount(test.inputRetryCount),
				WithRetryDelay(test.inputRetryDelay),
				WithRetryDelayMutation(test.inputDelayMutationFunc),
			)
			execDur := time.Since(start)

			require.Error(t, err)
			require.Less(t, execDur, test.wantMaxExecTime)
			require.Greater(t, execDur, test.wantMinExecTime)
		})
	}
}
