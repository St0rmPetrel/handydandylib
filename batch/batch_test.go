package batch

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func doVeryLongCalculation(ctx context.Context, req []int, opts ...grpc.CallOption) (int, error) {
	respSum := 0
	for _, elem := range req {
		respSum += elem
	}
	return respSum, nil
}

func TestGrpcHandler_DoConcurrent(t *testing.T) {

	tests := []struct {
		name           string
		inputData      []int
		batchSize      int
		goRoutinNum    int
		actulyResult   []int
		expectedResult []int
	}{
		{
			name:           "positive_empty",
			inputData:      []int{},
			batchSize:      3,
			goRoutinNum:    3,
			expectedResult: []int{},
		},
		{
			name:           "positive_invalid_batch_size",
			inputData:      []int{1, 2, 3, 4, 5},
			batchSize:      0,
			goRoutinNum:    3,
			expectedResult: []int{},
		},
		{
			name:           "positive_by_elem",
			inputData:      []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			batchSize:      1,
			goRoutinNum:    3,
			expectedResult: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name:           "positive_para",
			inputData:      []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			batchSize:      2,
			goRoutinNum:    3,
			expectedResult: []int{3, 7, 11, 15, 19},
		},
		{
			name:           "positive_triplet",
			inputData:      []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			batchSize:      3,
			goRoutinNum:    3,
			expectedResult: []int{6, 10, 15, 24},
		},
	}

	for _, test := range tests {
		indexIterator := NewBatch(0, len(test.inputData), test.batchSize)
		batchHandler := NewGrpcHandler(
			doVeryLongCalculation,
			func() (req []int, isLast, ok bool) {
				from, to, isLast, ok := indexIterator.Next()
				if !ok {
					return nil, false, false
				}
				req = test.inputData[from:to]
				return req, isLast, true
			},
		)
		t.Run(test.name, func(t *testing.T) {
			actulyResult, err := batchHandler.DoConcurrent(
				context.Background(),
				test.goRoutinNum,
			)
			assert.NoError(t, err)
			sort.Ints(actulyResult)
			assert.Equal(t, test.expectedResult, actulyResult)
		})

	}
}

func TestGrpcHandler_DoSerial(t *testing.T) {

	tests := []struct {
		name           string
		inputData      []int
		batchSize      int
		actulyResult   []int
		expectedResult []int
	}{
		{
			name:           "positive_empty",
			inputData:      []int{},
			batchSize:      3,
			expectedResult: []int{},
		},
		{
			name:           "positive_invalid_batch_size",
			inputData:      []int{1, 2, 3, 4, 5},
			batchSize:      0,
			expectedResult: []int{},
		},
		{
			name:           "positive_by_elem",
			inputData:      []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			batchSize:      1,
			expectedResult: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name:           "positive_para",
			inputData:      []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			batchSize:      2,
			expectedResult: []int{3, 7, 11, 15, 19},
		},
		{
			name:           "positive_triplet",
			inputData:      []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			batchSize:      3,
			expectedResult: []int{6, 15, 24, 10},
		},
	}

	for _, test := range tests {
		indexIterator := NewBatch(0, len(test.inputData), test.batchSize)
		serialBatch := NewGrpcHandler(
			doVeryLongCalculation,
			func() (req []int, isLast, ok bool) {
				from, to, isLast, ok := indexIterator.Next()
				if !ok {
					return nil, false, false
				}
				req = test.inputData[from:to]
				return req, isLast, true
			},
		)
		t.Run(test.name, func(t *testing.T) {
			actulyResult, err := serialBatch.DoSerial(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, test.expectedResult, actulyResult)
		})
	}
}

func TestSliceIndexIterator(t *testing.T) {
	type para struct {
		from int
		to   int
	}
	tests := []struct {
		name           string
		inputFrom      int
		inputTo        int
		inputBatchSize int
		expectedResult []para
	}{
		{
			name:           "positive_empty",
			inputFrom:      0,
			inputTo:        0,
			inputBatchSize: 2,
			expectedResult: []para{},
		},
		{
			name:           "positive_invalid_batch_size_1",
			inputFrom:      0,
			inputTo:        10,
			inputBatchSize: 0,
			expectedResult: []para{},
		},
		{
			name:           "positive_invalid_batch_size_2",
			inputFrom:      0,
			inputTo:        10,
			inputBatchSize: -1,
			expectedResult: []para{},
		},
		{
			name:           "positive_invalid_bound",
			inputFrom:      4,
			inputTo:        3,
			inputBatchSize: 1,
			expectedResult: []para{},
		},
		{
			name:           "positive_even_decimal",
			inputFrom:      0,
			inputTo:        10,
			inputBatchSize: 2,
			expectedResult: []para{
				{0, 2}, {2, 4}, {4, 6}, {6, 8}, {8, 10},
			},
		},
		{
			name:           "positive_odd_decimal",
			inputFrom:      0,
			inputTo:        10,
			inputBatchSize: 3,
			expectedResult: []para{
				{0, 3}, {3, 6}, {6, 9}, {9, 10},
			},
		},
		{
			name:           "positive_big_batch_size",
			inputFrom:      0,
			inputTo:        10,
			inputBatchSize: 30,
			expectedResult: []para{
				{0, 10},
			},
		},
		{
			name:           "positive_odd_not_from_begin",
			inputFrom:      53,
			inputTo:        73,
			inputBatchSize: 17,
			expectedResult: []para{
				{53, 70}, {70, 73},
			},
		},
	}
	accumulateResult := func(forwardIterator *Batch) []para {
		result := make([]para, 0)
		for {
			from, to, isLast, ok := forwardIterator.Next()
			if !ok {
				break
			}
			result = append(result, para{from, to})
			if isLast {
				break
			}
		}
		return result
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			it := NewBatch(test.inputFrom, test.inputTo, test.inputBatchSize)
			actulyResult := accumulateResult(it)
			assert.Equal(t, test.expectedResult, actulyResult)
		})
	}
}

func TestHandlerConfigValidate(t *testing.T) {
	tests := []struct {
		name             string
		inputBatchSize   int
		inputGoRoutinNum int
		actualIsValid    bool
	}{
		{
			name:             "positive_with_concurrent",
			inputBatchSize:   10,
			inputGoRoutinNum: 7,
			actualIsValid:    true,
		},
		{
			name:             "negative_not_enough_goroutin_num",
			inputBatchSize:   10,
			inputGoRoutinNum: 0,
			actualIsValid:    false,
		},
		{
			name:             "positive_without_concurrent",
			inputBatchSize:   10,
			inputGoRoutinNum: 1,
			actualIsValid:    true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewHandlerConfig(
				test.inputBatchSize,
				test.inputGoRoutinNum,
			)
			if test.actualIsValid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
