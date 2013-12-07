package indexes

import (
	"github.com/karlseguin/nabu/key"
)

// Interface for a sorted index
type Sort interface {
	Len() int
	CanRank() bool
	Load(ids []key.Type)
	Rank(id key.Type) (int, bool)
	Forwards(offset int) Iterator
	Backwards(offset int) Iterator
	Append(id key.Type)
	Prepend(id key.Type)
}

// Interface for a sorted index that can be dynamically updated
type DynamicSort interface {
	Set(id key.Type, rank int)
	Remove(id key.Type)
}

// Interface used to iterate over a sorted index.
//
// Note that Next advances then returns the next value
//
// Iteration returns key.NULL when done
type Iterator interface {
	Next() key.Type
	Current() key.Type
	Close()
}

// Creates a new sorted index. When length is -1, an index
// which supports dynamic updates will be returned. When length
// is less than the configured maxUnsortedSize, a static sort index
// will be used. Otherwise a static sort capable of ranking document
// is used. The choice between the possible implementation is a balance
// between performance, memory space and flexibility
func NewSort(length, maxUnsortedSize int) Sort {
	if length == -1 {
		return newSkiplist()
	}
	if length < maxUnsortedSize {
		return &StaticSort{}
	}
	return &StaticRankSort{}
}

// An iterator which returns no values
type EmptyIterator struct{}

func (i *EmptyIterator) Next() key.Type    { return key.NULL }
func (i *EmptyIterator) Current() key.Type { return key.NULL }
func (i *EmptyIterator) Close()            {}

var emptyIterator = &EmptyIterator{}
