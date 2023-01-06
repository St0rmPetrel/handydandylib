package batch

// LegcyBatch ...
type LegcyBatch struct {
	from, to, batchSize int64
}

// New ...
func New(from, to, batchSize int64) LegcyBatch {
	return LegcyBatch{from: from, to: to, batchSize: batchSize}
}

// Next ...
func (b *LegcyBatch) Next() (from, to int64, isLast, ok bool) {
	if b.from > b.to {
		return
	}

	ok = true

	from = b.from
	to = from + b.batchSize
	if to >= b.to {
		to = b.to
		isLast = true
	}

	b.from = to

	return
}
