package indexes

import (
	"github.com/karlseguin/nabu/key"
)

var (
	emptyIterator = new(EmptyIterator)
)

// an empty index
type Empty struct {
	name string
}

func NewEmpty(name string) *Empty {
	return &Empty{}
}

// Assumes the set is already read-locked
func (e *Empty) Name() string {
	return e.name
}

func (e *Empty) Set(id key.Type) {
}

func (e *Empty) Remove(id key.Type) {
}

// Get the number of documents indexed
// Assumes the set is already read-locked
func (e *Empty) Len() int {
	return 0
}

func (e *Empty) Contains(id key.Type) bool {
	return false
}

func (e *Empty) RLock() {
}

func (e *Empty) RUnlock() {
}

func (e *Empty) Score(id key.Type) (int, bool) {
	return 0, false
}

func (e *Empty) GetRank(score int, first bool) int {
	return 0
}

func (e *Empty) Forwards() Iterator {
	return emptyIterator
}

func (e *Empty) Backwards() Iterator {
	return emptyIterator
}

type EmptyIterator struct {
}

func (i *EmptyIterator) Next() key.Type {
	return key.NULL
}

func (i *EmptyIterator) Current() key.Type {
	return key.NULL
}

func (i *EmptyIterator) Offset(offset int) Iterator {
	return i
}

func (i *EmptyIterator) Range(from, to int) Iterator {
	return i
}

func (i *EmptyIterator) Close() {
}
