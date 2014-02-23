// Various sorted and non-sorted indexes for Nabu
package indexes

import (
	"github.com/karlseguin/nabu/key"
)

const MAX = 9223372036854775807
const MIN = -9223372036854775807

type Index interface {
	Name() string
	Len() int
	SetInt(id key.Type, score int)
	Remove(id key.Type)
	Contains(id key.Type) (int, bool)
	GetRank(score int, first bool) int
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
func NewIndex(name string, set bool) Index {
	if set {
		return newSet(name)
	}
	return newSkiplist(name)
}

// Creates the index
func LoadIndex(name string, values map[key.Type]int) Index {
	return loadSkiplist(name, values)
}

// An array of indexes
type Indexes []Index

func (i Indexes) Len() int {
	return len(i)
}

func (i Indexes) Less(a, b int) bool {
	return i[a].Len() < i[b].Len()
}

// Used to sort an array based on length
func (i Indexes) Swap(a, b int) {
	i[a], i[b] = i[b], i[a]
}

// Read locks all indexes within the array
func (indexes Indexes) RLock() {
	for _, index := range indexes {
		index.RLock()
	}
}

// Read unlocks all indexes within the array
func (indexes Indexes) RUnlock() {
	for _, index := range indexes {
		index.RUnlock()
	}
}
