package indexes

import (
	"github.com/karlseguin/nabu/key"
)

// Interface for a sorted index
type Sort interface {
	Len() int
	CanScore() bool
	Load(ids []key.Type)
	GetScore(id key.Type) (int, bool)
	Forwards() Iterator
	Backwards() Iterator
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
	Offset(offset int) Iterator
	Range(from, to int) Iterator
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
	return &StaticScoreSort{}
}
