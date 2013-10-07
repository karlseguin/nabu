package nabu

import (
  "sync"
)

type Change struct {
  id string
  added bool
  indexName string
}

type ChangeBucket struct {
  sync.RWMutex
  indexName string
  items map[string]*CacheItem
}

func newChangeBucket(indexName string) *ChangeBucket {
  return &ChangeBucket {
    indexName: indexName,
    items: make(map[string]*CacheItem),
  }
}

func (cb *ChangeBucket) add(item *CacheItem) {
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
