package retry

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDo(t *testing.T) {
	var accum int

	tests := []struct {
		name               string
		inputFailCallBack  func(uint, error)
		inputRetryCount    uint
		inputRetryDelay    time.Duration
		inputRetryableFunc func() error
		wantMaxExecTime    time.Duration
		isPositive         bool
		wantAccum          int
	}{
		{
			name:               "negative_no_retry",
			isPositive:         false,
			inputRetryCount:    0,
			inputRetryableFunc: func() error { return fmt.Errorf("") },
			inputFailCallBack:  func(attemt uint, _ error) { accum = int(attemt) },
			wantAccum:          1,
			inputRetryDelay:    5 * time.Second,
			wantMaxExecTime:    1 * time.Second,
		},
		{
			name:               "positive_no_retry",
			isPositive:         true,
			inputRetryCount:    0,
			inputRetryableFunc: func() error { accum = 5; return nil },
			inputFailCallBack:  func(_ uint, _ error) { accum = -100 },
			wantAccum:          5,
			inputRetryDelay:    5 * time.Second,
			wantMaxExecTime:    1 * time.Second,
		},
		{
			name:               "negative_one_retry",
			isPositive:         false,
			inputRetryCount:    1,
			inputRetryableFunc: func() error { return fmt.Errorf("") },
			inputFailCallBack:  func(attemt uint, _ error) { accum = int(attemt) },
			wantAccum:          2,
			inputRetryDelay:    100 * time.Millisecond,
			wantMaxExecTime:    200 * time.Millisecond,
		},
		{
			name:            "positive_one_retry",
			isPositive:      true,
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
			name:            "positive_many_retry_fibonacci",
			isPositive:      true,
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
			assert.Equal(t, test.wantAccum, accum)
			assert.Less(t, execDur, test.wantMaxExecTime)
			if !test.isPositive {
				assert.Error(t, err)
				return
			}
		})
	}
}
