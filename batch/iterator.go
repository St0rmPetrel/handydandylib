package batch

import "sync"

// Batch итератор по батчам индекса слайса
type SegmentIterator struct {
	sync.Mutex
	fromFloting, toRightBound, step int
}

// NewSegmentIterator конструктор итератора отрезка [from, to)
// принимает левую границу итераций (включая), правую границу итерации (не включая)
// инкремент итерации
// Пример:
// i := bath.NewSegmentIterator(0, 10, 4)
// (0  ,  4, true)
// from, to, ok := i.Next()
// (4  ,  8, true)
// from, to, ok = i.Next()
// (8  , 10, true)
// from, to, ok = i.Next()
// (0  ,  0, false)
// from, to, ok = i.Next()
func NewSegmentIterator(from, to, step int) *SegmentIterator {
	return &SegmentIterator{fromFloting: from, toRightBound: to, step: step}
}

// Next следующая итерация
func (i *SegmentIterator) Next() (from, to int, ok bool) {
	i.Lock()
	defer i.Unlock()
	if i.step <= 0 {
		return 0, 0, false
	}
	if i.fromFloting >= i.toRightBound {
		return 0, 0, false
	}

	from = i.fromFloting
	to = from + i.step
	i.fromFloting = to

	if to >= i.toRightBound {
		to = i.toRightBound
	}
	return from, to, true
}
