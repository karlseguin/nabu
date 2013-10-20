package cache

import (
  "sync"
  "nabu/key"
)

type Change struct {
  id key.Type
  added bool
  indexName string
}

type ChangeBucket struct {
  sync.RWMutex
  indexName string
  items map[string]*Item
}

func newChangeBucket(indexName string) *ChangeBucket {
  return &ChangeBucket {
    indexName: indexName,
    items: make(map[string]*Item),
  }
}

func (cb *ChangeBucket) add(item *Item) {
  cb.Lock()
  defer cb.Unlock()
  cb.items[item.key] = item
}

func (cb *ChangeBucket) process(change *Change) {
  cb.RLock()
  defer cb.RUnlock()
  for _, item := range cb.items{
    item.change(change)
  }
}

func (cb *ChangeBucket) remove(item *Item) {
  cb.Lock()
  defer cb.Unlock()
  delete(cb.items, item.key)
}
