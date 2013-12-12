// Various sorted and non-sorted indexes for Nabu
package indexes

import (
	"github.com/karlseguin/nabu/key"
)

type Index interface {
	Add(id key.Type)
	Remove(id key.Type) int
	Name() string
	Len() int
	Contains(id key.Type) bool
	Ids() map[key.Type]struct{}

	RLock()
	RUnlock()
}

// Creates the index
func NewIndex(name string) Index {
	return &SimpleIndex{
		name: name,
		ids:  make(map[key.Type]struct{}),
	}
}

// Creates the index
func LoadIndex(name string, ids map[key.Type]struct{}) Index {
	return &SimpleIndex{
		name: name,
		ids:  ids,
	}
}

// An array of indexes
type Indexes []Index

// The number of items in our array of indexes
func (indexes Indexes) Len() int {
	return len(indexes)
}

// Used to sort an array based on length
func (indexes Indexes) Less(i, j int) bool {
	return indexes[i].Len() < indexes[j].Len()
}

// Used to sort an array based on length
func (indexes Indexes) Swap(i, j int) {
	x := indexes[i]
	indexes[i] = indexes[j]
	indexes[j] = x
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
