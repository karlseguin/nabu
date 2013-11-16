// Various sorted and non-sorted indexes for Nabu
package indexes

import (
  "sync"
  "github.com/karlseguin/nabu/key"
)

// A non-sorted index (essentially a set)
type Index struct {
  sync.RWMutex
  Name string
  Ids map[key.Type]struct{}
}

// Creates the index
func New(name string) *Index {
  return &Index{
    Name: name,
    Ids: make(map[key.Type]struct{}),
  }
}

// Add an id to the index
func (i *Index) Add(id key.Type) {
  i.Lock()
  defer i.Unlock()
  i.Ids[id] = struct{}{}
}

// Remove an id from the index
func (i *Index) Remove(id key.Type) int {
  i.Lock()
  defer i.Unlock()
  delete(i.Ids, id)
  return len(i.Ids)
}

// Determine whether or not the index contains an item
func (i *Index) Contains(id key.Type) bool {
  i.RLock()
  defer i.RUnlock()
  _, exists := i.Ids[id]
  return exists
}

// Number of documents in the index
func (i *Index) Len() int {
  i.RLock()
  defer i.RUnlock()
  return len(i.Ids)
}

// An array of indexes
type Indexes []*Index

// The number of items in our array of indexes
func (indexes Indexes) Len() int {
  return len(indexes)
}

// Used to sort an array based on length
func (indexes Indexes) Less(i, j int) bool {
  return len(indexes[i].Ids) < len(indexes[j].Ids)
}

// Used to sort an array based on length
func (indexes Indexes) Swap(i, j int) {
  x := indexes[i]
  indexes[i] = indexes[j]
  indexes[j] = x
}

// Read locks all indexes within the array
func (indexes Indexes) RLock() {
  for _, index := range indexes { index.RLock() }
}

// Read unlocks all indexes within the array
func (indexes Indexes) RUnlock() {
  for _, index := range indexes { index.RUnlock() }
}
