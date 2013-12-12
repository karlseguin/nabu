package indexes

import (
	"github.com/karlseguin/nabu/key"
	"sync"
)

// A non-sorted index which is only capable
// of doing membership checks
type SimpleIndex struct {
	sync.RWMutex
	name string
	ids  map[key.Type]struct{}
}

// Add an id to the index
func (i *SimpleIndex) Add(id key.Type) {
	i.Lock()
	defer i.Unlock()
	i.ids[id] = struct{}{}
}

// Remove an id from the index
func (i *SimpleIndex) Remove(id key.Type) int {
	i.Lock()
	defer i.Unlock()
	delete(i.ids, id)
	return i.Len()
}

// Gets the ids belonging to this index
// Assumes the index is already locked
func (i *SimpleIndex) Ids() map[key.Type]struct{} {
	return i.ids
}

// Determine whether or not the index contains an item
// Assumes the index is already locked
func (i *SimpleIndex) Contains(id key.Type) bool {
	_, exists := i.ids[id]
	return exists
}

// Number of documents in the index
// Assumes the index is already locked
func (i *SimpleIndex) Len() int {
	return len(i.ids)
}

func (i *SimpleIndex) Name() string {
	return i.name
}
