package cache

import (
	"github.com/karlseguin/nabu/key"
	"sync"
)

// Represents a change to an index
type Change struct {
	added     bool
	id        key.Type
	indexName string
}

// A mapping of all the cached index which are comprised of an actual index
type ChangeBucket struct {
	sync.RWMutex
	indexName string
	items     map[string]*Item
}

func newChangeBucket(indexName string) *ChangeBucket {
	return &ChangeBucket{
		indexName: indexName,
		items:     make(map[string]*Item),
	}
}

// Add a cache index which is associated with the actual index
func (cb *ChangeBucket) add(item *Item) {
	cb.Lock()
	defer cb.Unlock()
	cb.items[item.key] = item
}

// update all of the cached indexes
func (cb *ChangeBucket) process(change *Change) {
	cb.RLock()
	defer cb.RUnlock()
	for _, item := range cb.items {
		item.change(change)
	}
}

// remove an actual index from this association
func (cb *ChangeBucket) remove(item *Item) {
	cb.Lock()
	defer cb.Unlock()
	delete(cb.items, item.key)
}
