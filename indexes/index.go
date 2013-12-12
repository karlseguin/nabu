// Various sorted and non-sorted indexes for Nabu
package indexes

import (
	"github.com/karlseguin/nabu/key"
)

type Index interface {
	Name() string
	Len() int
	SetInt(id key.Type, score int)
	Remove(id key.Type) int
	Contains(id key.Type) (int, bool)
	RLock()
	RUnlock()

	Forwards() Iterator
	Backwards() Iterator
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


// Creates the index
func NewIndex(name string) Index {
	return newSkiplist(name)
}

// Creates the index
func LoadIndex(name string, values map[key.Type]int) Index {
	return loadSkiplist(name, values)
}

// An array of indexes
type Indexes []Index
