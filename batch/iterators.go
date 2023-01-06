package batch

import "sync"

// Batch итератор по батчам индекса слайса
type Batch struct {
	sync.Mutex
	fromFloting, toRightBound, batchSize int
}

// NewBatch конструктор итератора по батчам индекса слайса
// принимает левую границу итераций (включая), правую границу итерации (не включая)
// инкремент итерации
func NewBatch(from, to, batchSize int) *Batch {
	return &Batch{fromFloting: from, toRightBound: to, batchSize: batchSize}
}

// Next новая итерация
func (b *Batch) Next() (from, to int, isLast, ok bool) {
	b.Lock()
	defer b.Unlock()
	if b.fromFloting >= b.toRightBound || b.batchSize <= 0 {
		return 0, 0, false, false
	}

	from = b.fromFloting
	to = from + b.batchSize
	b.fromFloting = to

	if to >= b.toRightBound {
		return from, b.toRightBound, true, true
	}
	return from, to, false, true
}
